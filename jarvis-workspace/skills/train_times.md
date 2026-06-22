---
name: train_times
when_to_use: When the user asks about UK train times, departures, or how to get between two places by train — e.g. "next trains from X to Y", "how do I get to Z by 9am", "last train home", "is the 0815 delayed".
allowed_tools: [fetch_page]
required: [request]
---

Find real UK train times by fetching a live Realtime Trains board and reading it.
Always read the fetched page — never answer train times from memory.

1. Work out the FROM and TO stations as three-letter CRS codes. Common ones:
   Effingham Junction=EFF, Bookham=BKA, Guildford=GLD, Woking=WOK, Clandon=CLA,
   Horsley=HSY, Leatherhead=LHD, Dorking=DKG, Epsom=EPS, Surbiton=SUR,
   Clapham Junction=CLJ, London Waterloo=WAT, London Vauxhall=VXH, London Victoria=VIC.
   You'll know most others; if truly unsure, don't guess.

2. Work out the DATE (use the current date you've been given for today/tomorrow,
   as yyyy-mm-dd) and a search TIME as HHMM (24h):
   - "next trains" / "now" → the current time.
   - "leave after HH:MM" / "this evening" → that time.
   - "arrive by HH:MM" → an arrival deadline; the train must leave well before it, so
     search about 90 minutes earlier and prefer the latest departures that still get
     there in time.

3. fetch_page this URL (note the date uses dashes):
   https://www.realtimetrains.co.uk/search/detailed/gb-nr:<FROM>/to/gb-nr:<TO>/<yyyy-mm-dd>/<HHMM>

4. Read the returned text. Each direct service shows its booked departure from <FROM>
   and, if running late/cancelled, its real time too — so report delays and
   cancellations, not just the timetable. Give the few departures that answer the
   question (next 3–5, or those meeting the deadline), each as "HH:MM" with its status
   (on time / N late / cancelled) and where it's going.

5. If the board has no direct services, say so plainly (it may need a change) rather
   than inventing times. Keep the reply short and to the point.
