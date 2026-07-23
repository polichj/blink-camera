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
Streaming at tcp://127.0.0.1:8554 (point OBS/ffplay here)
```

and keeps running, automatically re-requesting a fresh live view session
if the stream ever drops.

Optional flags: `--host` and `--port` to change the local bind address
(default `127.0.0.1:8554`).

## OBS setup

Add Source → Media Source → uncheck "Local File" → Input:
`tcp://127.0.0.1:8554` → Input Format: `mpegts`.

To sanity-check the stream is flowing before wiring up OBS:

```bash
ffplay -f mpegts tcp://127.0.0.1:8554
```

## Notes

- Never commit `blink_session.json` — it contains your live auth token.
- Don't reduce `--host`/`--port` polling below what's needed; Blink's API
  isn't meant to be hit faster than necessary (blinkpy's internal
  keep-alive already handles this safely).
