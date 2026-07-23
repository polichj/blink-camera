"""
Keeps a Blink camera's live view session alive indefinitely and exposes it as
a local MPEG-TS stream that OBS (or ffplay/VLC) can consume directly.

Usage:
    python blink_stream.py "Front Door"

First run prompts for your Blink username/password and a 2FA code (emailed
by Blink). The resulting session token is cached in blink_session.json so
later runs skip the login prompt.

In OBS: Add Source -> Media Source -> uncheck "Local File" -> Input:
tcp://127.0.0.1:8554 -> Input Format: mpegts
"""

import argparse
import asyncio
import json
import logging
import os
import sys

from aiohttp import ClientSession
from blinkpy.auth import Auth, BlinkTwoFARequiredError, LoginError
from blinkpy.blinkpy import Blink

SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "blink_session.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("blinkpy").setLevel(logging.WARNING)
log = logging.getLogger("blink_stream")


async def login(session: ClientSession) -> Blink:
    blink = Blink(session=session)

    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE) as f:
            login_data = json.load(f)
        blink.auth = Auth(login_data, no_prompt=True)
        try:
            await blink.start()
            log.info("Logged in using cached session.")
            return blink
        except LoginError:
            log.warning("Cached session is no longer valid, logging in again.")

    try:
        await blink.start()
    except BlinkTwoFARequiredError:
        await blink.prompt_2fa()

    await blink.save(SESSION_FILE)
    log.info("Logged in and cached session to %s", SESSION_FILE)
    return blink


async def stream_camera(blink: Blink, camera_name: str, host: str, port: int) -> None:
    if camera_name not in blink.cameras:
        available = ", ".join(blink.cameras.keys())
        raise SystemExit(f'Camera "{camera_name}" not found. Available cameras: {available}')

    camera = blink.cameras[camera_name]

    while True:
        try:
            log.info("Requesting live view session for %s...", camera_name)
            live_stream = await camera.init_livestream()
            server = await live_stream.start(host=host, port=port)
            sockname = server.sockets[0].getsockname()
            log.info("Streaming at tcp://%s:%s (point OBS/ffplay here)", sockname[0], sockname[1])
            await live_stream.feed()
            log.warning("Stream ended, restarting session in 3s...")
        except Exception:
            log.exception("Live view session failed, retrying in 3s...")
        await asyncio.sleep(3)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("camera_name", help='Camera name exactly as shown in the Blink app, e.g. "Front Door"')
    parser.add_argument("--host", default="127.0.0.1", help="Local host to bind the stream server to")
    parser.add_argument("--port", type=int, default=8554, help="Local port to bind the stream server to")
    args = parser.parse_args()

    async with ClientSession() as session:
        blink = await login(session)
        await stream_camera(blink, args.camera_name, args.host, args.port)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
