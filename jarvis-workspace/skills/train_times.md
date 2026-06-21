---
name: train_times
when_to_use: When the user asks about UK train times, departures, or how to get between two places by train — e.g. "next trains from X to Y", "how do I get to Z by 9am", "last train home", "trains to Waterloo tomorrow morning".
allowed_tools: [browser_open, browser_read, browser_snapshot]
required: [request]
---

Find real UK train times by reading a live board on Realtime Trains. Always read
the actual board — never answer train times from memory or a search snippet.

1. Work out the FROM and TO stations and their three-letter CRS codes. Common ones:
   Effingham Junction=EFF, Bookham=BKA, Guildford=GLD, Woking=WOK, Clandon=CLA,
   Horsley=HSY, Leatherhead=LHD, Dorking=DKG, Epsom=EPS, Surbiton=SUR,
   Clapham Junction=CLJ, London Waterloo=WAT, London Vauxhall=VXH, London Victoria=VIC.
   You will know most UK codes. If you are genuinely unsure of one, find it on
   Realtime Trains itself (browser_open https://www.realtimetrains.co.uk/search and read
   the station list) — never guess a code, and never fall back to web_search for the
   times themselves: always read them off the live board below.

2. Work out the DATE (use the current date you've been given for "today"/"tomorrow",
   format yyyy-mm-dd) and a single search TIME as HHMM (24-hour). Realtime Trains shows
   DEPARTURES from the search time onward — it has NO "arrive by" mode — so choose the
   search time by intent:
   - "next trains" / "now" → the current time.
   - "leave after HH:MM" / "trains at HH:MM" / "this evening" → that departure time.
   - "arrive by / for / to get there by HH:MM" → an ARRIVAL deadline. A train must
     DEPART well before the deadline to arrive in time, so search about TWO HOURS
     BEFORE the deadline (e.g. deadline 09:00 → search 0700), as a single HHMM, never a
     range that ends at the deadline. Read the board's ARRIVAL column and keep only
     trains arriving at or before the deadline; report the LATEST one or two that still
     make it, plus one earlier backup. (Searching at or near the deadline is wrong —
     those trains arrive after it.)

3. Open the live board:
   browser_open https://www.realtimetrains.co.uk/search/detailed/gb-nr:<FROM>/to/gb-nr:<TO>/<yyyy-mm-dd>/<HHMM>
   Then browser_read the page. (If the board looks empty or the station is wrong,
   browser_snapshot to check, or fix the CRS code and reopen — don't invent times.)

4. Read the actual rows. For each relevant service report: departure time, arrival
   time, and whether it's direct or how many changes (and where). Note the operator
   only if useful. Pick the trains that genuinely answer the question (e.g. the next
   3–5 departures, or the ones that meet an arrival deadline).

5. Return the result as clear data the assistant can relay: each option as
   "HH:MM → HH:MM (direct)" or "HH:MM → HH:MM (change at <station>)". If you could not
   confirm times from the board, say so plainly rather than guessing.
