"""Text console connector — drive the brain from a terminal (no mic/STT/TTS).

A developer convenience AND the headless test harness. It connects to the brain
over the SAME WebSocket protocol the voice intercom uses, but sends
`TextIn(text_only=True)` and prints `ReplyText` — the brain skips TTS for the turn,
so this needs no audio stack and no TTS key. It exercises the whole brain (persona,
tools, background lane, browser) as text:

  jarvis text                      # interactive REPL (or piped stdin)
  jarvis text --once "do the thing" # one turn, print the reply, exit (scriptable)

A thin boundary peer like the WhatsApp connector — it imports nothing from the
brain and pairs with the device id + intercom token.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid

from jarvis.config import Config
from jarvis.protocol.messages import (
    Hello,
    Proactive,
    ReplyEnd,
    ReplyText,
    TextIn,
    Transcript,
    Welcome,
    decode,
    encode,
)


async def text_turn(ws, text: str) -> tuple[str, bool]:  # noqa: ANN001
    """Drive ONE text turn through the brain and return (reply, ended). Sends a
    text-only TextIn and collects ReplyText up to ReplyEnd; prints any Proactive
    push (a background/heartbeat result) that arrives first. Pure routing —
    unit-tested with a fake socket."""
    turn_id = uuid.uuid4().hex
    await ws.send(encode(TextIn(turn_id=turn_id, text=text, text_only=True)))
    reply, ended = "", False
    async for raw in ws:
        m = decode(raw)
        if isinstance(m, ReplyText) and m.turn_id == turn_id:
            reply = m.text
        elif isinstance(m, ReplyEnd) and m.turn_id == turn_id:
            ended = m.ended
            break
        elif isinstance(m, Proactive):
            print(f"\n🔔 {m.text}\n")
    return reply, ended


class TextConsole:
    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg

    async def _connect(self):  # noqa: ANN202 - returns (ws, Welcome)
        import websockets

        ws = await websockets.connect(self._cfg.intercom.brain_url, open_timeout=5)
        await ws.send(
            encode(
                Hello(
                    device_id=self._cfg.capabilities.device_id,
                    token=self._cfg.intercom.token.get_secret_value(),
                    channel="text",
                )
            )
        )
        welcome = decode(await asyncio.wait_for(ws.recv(), 5))
        if not isinstance(welcome, Welcome):
            await ws.close()
            raise RuntimeError(f"pairing rejected: {welcome}")
        return ws, welcome

    async def once(self, text: str) -> int:
        """Send one message, print the reply, exit — the scriptable path."""
        ws, _ = await self._connect()
        try:
            reply, _ended = await text_turn(ws, text)
            print(reply)
        finally:
            await ws.close()
        return 0

    async def _connect_retry(self, *, announce: bool):  # noqa: ANN202 - returns (ws, Welcome)
        """Connect to the brain, retrying with backoff so a brain restart/outage doesn't
        kill the console — mirrors the intercom + whatsapp reconnect behaviour."""
        while True:
            try:
                ws, welcome = await self._connect()
            except Exception as exc:  # noqa: BLE001 - brain down / not yet up / rejected
                print(f"  [text] can't reach brain ({type(exc).__name__}); retrying in 3s…")
                await asyncio.sleep(3)
                continue
            if announce:
                caps = ", ".join(welcome.capabilities) or "(none)"
                print(
                    f"jarvis text → {self._cfg.intercom.brain_url}\n"
                    f"paired as {welcome.identity} ({welcome.scope}); can: {caps}\n"
                    "Type a message, or Ctrl-D to exit.\n"
                )
            else:
                print("  [text] reconnected.")
            return ws, welcome

    async def repl(self) -> int:
        """Interactive (or piped) REPL. A single router task reads the socket — turn
        replies go to a queue, and Proactive pushes (alarms, background completions,
        heartbeat) print AS THEY ARRIVE, even between turns. That continuous reader is
        what makes notification delivery work; a turn-only reader would miss them.

        The brain link auto-reconnects: each turn races the reader task, so a dropped
        socket (brain restart) rebuilds the connection and retries the turn rather than
        hanging or exiting."""
        ws, _ = await self._connect_retry(announce=True)
        turn_q: asyncio.Queue = asyncio.Queue()
        reader = asyncio.create_task(self._route(ws, turn_q))
        try:
            while True:
                line = await asyncio.to_thread(sys.stdin.readline)
                if not line:  # EOF (Ctrl-D / end of pipe)
                    break
                text = line.strip()
                if not text:
                    continue
                while True:  # retry the turn across reconnects
                    turn = asyncio.create_task(self._turn(ws, turn_q, text))
                    done, _ = await asyncio.wait(
                        {turn, reader}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if turn in done and not turn.exception():
                        reply, ended = turn.result()
                        print(f"jarvis: {reply}")
                        if ended:
                            print("(conversation ended)")
                        break
                    # link dropped (reader ended, or the send raised): rebuild + retry
                    turn.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await turn
                    print("  [text] brain link lost; reconnecting…")
                    reader.cancel()
                    with contextlib.suppress(Exception):
                        await ws.close()
                    ws, _ = await self._connect_retry(announce=False)
                    turn_q = asyncio.Queue()
                    reader = asyncio.create_task(self._route(ws, turn_q))
        finally:
            reader.cancel()
            with contextlib.suppress(Exception):
                await ws.close()
        return 0

    async def _route(self, ws, turn_q: asyncio.Queue) -> None:  # noqa: ANN001
        """The single socket reader: turn frames → queue; proactive pushes → print now."""
        try:
            async for raw in ws:
                m = decode(raw)
                if isinstance(m, Proactive):
                    print(f"\n🔔 {m.text}\n", flush=True)
                elif isinstance(m, (ReplyText, ReplyEnd, Transcript)) and not m.turn_id.startswith("pa-"):
                    await turn_q.put(m)
                # ReplyAudio (and proactive 'pa-' trailing frames) are ignored in text mode
        except Exception:  # noqa: BLE001 - socket closed / shutting down
            pass

    @staticmethod
    async def _turn(ws, turn_q: asyncio.Queue, text: str) -> tuple[str, bool]:  # noqa: ANN001
        turn_id = uuid.uuid4().hex
        await ws.send(encode(TextIn(turn_id=turn_id, text=text, text_only=True)))
        reply, ended = "", False
        while True:
            m = await turn_q.get()
            if isinstance(m, ReplyText) and m.turn_id == turn_id:
                reply = m.text
            elif isinstance(m, ReplyEnd) and m.turn_id == turn_id:
                ended = m.ended
                break
        return reply, ended
