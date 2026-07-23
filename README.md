# blink-camera

Keeps a Blink camera's live view session alive indefinitely (bypassing the
normal 30s/5min timeout) and exposes it as a local MPEG-TS stream for OBS
to consume, e.g. for restreaming.

Built on [blinkpy](https://github.com/fronzbot/blinkpy), which talks to
Blink's real API rather than automating the app or web portal (the web
portal doesn't support live view at all).

## Setup

```bash
git clone https://github.com/polichj/blink-camera.git
cd blink-camera
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python blink_stream.py "Your Camera Name Here"
```

Use the camera name exactly as it appears in the Blink app.

First run prompts for your Blink username/password, then a 2FA code
(emailed by Blink). The resulting session token is cached in
`blink_session.json` (gitignored, never committed) so later runs skip the
login prompt.

The script prints something like:

```
Streaming (buffered 20.0s behind live) at tcp://192.168.1.50:8554 (point OBS/ffplay here)
```

and keeps running, automatically re-requesting a fresh live view session
if the stream ever drops. The local server itself stays up across those
reconnects — OBS only needs to connect once.

Output is buffered up to `--buffer-seconds` (default `20`) behind real
time. Blink's live view periodically drops or stalls; the buffer holds a
cushion of already-received footage so those gaps get absorbed rather than
replayed — a stall shorter than the buffered cushion becomes invisible to
viewers instead of showing up `--buffer-seconds` later. An outage that
drains the whole cushion still shows up as a real pause. Raise
`--buffer-seconds` for more resilience at the cost of more lag, or lower it
for less lag.

By default it binds to `0.0.0.0`, so the stream is reachable both from
this machine and from other machines on your LAN (e.g. a separate
OBS/capture-card PC) — the printed address reflects whichever applies.
Pass `--host 127.0.0.1` to restrict it to this machine only. `--port`
changes the bind port (default `8554`).

## OBS setup

Add Source → Media Source → uncheck "Local File" → Input:
`tcp://<printed-address>:8554` → Input Format: `mpegts`.

If OBS is on a different machine than the script, make sure Windows
Firewall (or your OS firewall) allows inbound TCP on that port.

To sanity-check the stream is flowing before wiring up OBS:

```bash
ffplay -f mpegts tcp://<printed-address>:8554
```

## Notes

- Never commit `blink_session.json` — it contains your live auth token.
- Don't reduce `--host`/`--port` polling below what's needed; Blink's API
  isn't meant to be hit faster than necessary (blinkpy's internal
  keep-alive already handles this safely).
