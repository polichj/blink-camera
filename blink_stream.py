"""
Keeps a Blink camera's live view session alive indefinitely and exposes it as
a local MPEG-TS stream that OBS (or ffplay/VLC) can consume directly.

Usage:
    python blink_stream.py "Front Door"

First run prompts for your Blink username/password and a 2FA code (emailed
by Blink). The resulting session token is cached in blink_session.json so
later runs skip the login prompt.

By default the stream server binds to 0.0.0.0, so it's reachable from other
machines on your LAN (e.g. a separate OBS/capture-card PC) as well as
locally. The log line at startup prints the address to use.

Output is buffered up to --buffer-seconds (default 20) behind real time.
Blink's live view periodically drops/stalls for anywhere from a second to
several minutes; the buffer holds a cushion of already-received footage so
those gaps can be absorbed rather than replayed -- an upstream stall
shorter than the buffered cushion becomes invisible to viewers instead of
reappearing --buffer-seconds later. An outage that drains the whole
cushion still shows up as a real pause. The local server itself stays up
across upstream reconnects, so OBS only needs to connect once.

Absorbed gaps are also removed from the stream's own embedded clock (PCR
and PTS/DTS in each TS packet are shifted by the same accumulated amount),
so the demuxer sees a steady clock instead of a stall-then-catch-up, which
is what otherwise shows up as a brief rate wobble even after the gap
itself is hidden. See _restamp_chunk.

In OBS: Add Source -> Media Source -> uncheck "Local File" -> Input:
tcp://<printed-address>:8554 -> Input Format: mpegts

Pass --host 127.0.0.1 to restrict the stream to only this machine.
"""

import argparse
import asyncio
import json
import logging
import os
import socket
import ssl
import sys
import time

from aiohttp import ClientSession
from blinkpy.auth import Auth, BlinkTwoFARequiredError, LoginError
from blinkpy.blinkpy import Blink

SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "blink_session.json")

LIVEVIEW_REQUEST_TIMEOUT = 15  # seconds allowed for requesting/authing a live view session
READ_IDLE_TIMEOUT = 20  # seconds of no video data before treating the connection as dead

# 188-byte MPEG-TS null packet (PID 0x1FFF): players are required to discard these, but
# their presence keeps the byte-stream moving during upstream gaps instead of going fully
# silent, which can otherwise trip a demuxer's own idle/discontinuity handling.
NULL_TS_PACKET = bytes([0x47, 0x1F, 0xFF, 0x10]) + bytes([0xFF] * 184)
KEEPALIVE_INTERVAL = 0.5  # seconds between null packets while no real data is due
GAP_CAP_SECONDS = 0.05  # max visible pause per item during steady-state playback; backlog absorbs the rest
REPLENISH_FRACTION = 0.1  # during healthy periods, wait up to this much extra (as a fraction of the
# real gap) to slowly rebuild the delay cushion after gap-capping has spent it absorbing a stall
THROUGHPUT_REPORT_INTERVAL = 3  # seconds between "Dispatch throughput" log lines

TS_PACKET_SIZE = 188
PCR_TICKS_PER_SEC = 90_000  # PCR base, PTS, and DTS all tick at 90kHz
TIMESTAMP_MOD = 1 << 33  # PCR base, PTS, and DTS all wrap at 33 bits

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("blinkpy").setLevel(logging.WARNING)
log = logging.getLogger("blink_stream")


def lan_ip() -> str:
    """Best-effort guess at this machine's LAN IP (for display only)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except OSError:
            return socket.gethostbyname(socket.gethostname())


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


async def _drain_livestream_into_buffer(live_stream, queue: asyncio.Queue) -> None:
    """Read IMMI-framed video from one live view session into the shared buffer.

    Uses readexactly() rather than blinkpy's own BlinkLiveStream.recv(),
    which calls StreamReader.read(n) and misreads an ordinary short TCP read
    as a fatal framing error, killing the session within seconds on any
    connection that isn't perfect. See
    https://github.com/fronzbot/blinkpy/issues/1262.
    """
    reader = live_stream.target_reader
    last_frame_time = None
    try:
        while not reader.at_eof():
            try:
                header = await asyncio.wait_for(reader.readexactly(9), READ_IDLE_TIMEOUT)
            except (asyncio.IncompleteReadError, asyncio.TimeoutError):
                break

            msgtype = header[0]
            payload_length = int.from_bytes(header[5:9], byteorder="big")
            if payload_length <= 0:
                continue

            try:
                data = await asyncio.wait_for(reader.readexactly(payload_length), READ_IDLE_TIMEOUT)
            except (asyncio.IncompleteReadError, asyncio.TimeoutError):
                break

            if msgtype != 0x00 or data[0] != 0x47:
                continue

            now = time.monotonic()
            if last_frame_time is not None and (gap := now - last_frame_time) > 0.5:
                log.warning("Gap of %.2fs between video frames from Blink (before local buffering)", gap)
            last_frame_time = now

            await queue.put((now, data))
    except ssl.SSLError as e:
        if e.reason != "APPLICATION_DATA_AFTER_CLOSE_NOTIFY":
            log.exception("SSL error while receiving upstream data")
    except Exception:
        log.exception("Error while receiving upstream data")
    finally:
        live_stream.target_writer.close()


async def ingest_loop(camera, camera_name: str, queue: asyncio.Queue) -> None:
    """Keep re-establishing the Blink live view session and feeding the buffer."""
    while True:
        try:
            log.info("Requesting live view session for %s...", camera_name)
            live_stream = await asyncio.wait_for(camera.init_livestream(), LIVEVIEW_REQUEST_TIMEOUT)
            await asyncio.wait_for(live_stream.auth(), LIVEVIEW_REQUEST_TIMEOUT)
            await asyncio.gather(
                _drain_livestream_into_buffer(live_stream, queue),
                live_stream.send(),
                live_stream.poll(),
            )
            log.warning("Upstream session ended, reconnecting in 3s...")
        except Exception:
            log.exception("Upstream session failed, retrying in 3s...")
        await asyncio.sleep(3)


def _rewrite_pcr(packet, offset: int, warp_ticks: int) -> None:
    base = (
        (packet[offset] << 25)
        | (packet[offset + 1] << 17)
        | (packet[offset + 2] << 9)
        | (packet[offset + 3] << 1)
        | (packet[offset + 4] >> 7)
    )
    ext = ((packet[offset + 4] & 0x1) << 8) | packet[offset + 5]
    new_base = (base - warp_ticks) % TIMESTAMP_MOD
    packet[offset] = (new_base >> 25) & 0xFF
    packet[offset + 1] = (new_base >> 17) & 0xFF
    packet[offset + 2] = (new_base >> 9) & 0xFF
    packet[offset + 3] = (new_base >> 1) & 0xFF
    packet[offset + 4] = ((new_base & 0x1) << 7) | 0x7E | ((ext >> 8) & 0x1)
    packet[offset + 5] = ext & 0xFF


def _rewrite_timestamp(packet, offset: int, warp_ticks: int) -> None:
    value = (
        ((packet[offset] >> 1) & 0x07) << 30
        | packet[offset + 1] << 22
        | ((packet[offset + 2] >> 1) & 0x7F) << 15
        | packet[offset + 3] << 7
        | ((packet[offset + 4] >> 1) & 0x7F)
    )
    new_value = (value - warp_ticks) % TIMESTAMP_MOD
    marker = packet[offset] & 0xF1  # keep the 4-bit prefix and marker bit, replace the timestamp bits
    packet[offset] = marker | (((new_value >> 30) & 0x07) << 1)
    packet[offset + 1] = (new_value >> 22) & 0xFF
    packet[offset + 2] = (((new_value >> 15) & 0x7F) << 1) | 0x1
    packet[offset + 3] = (new_value >> 7) & 0xFF
    packet[offset + 4] = ((new_value & 0x7F) << 1) | 0x1


def _restamp_ts_packet(packet, warp_ticks: int) -> None:
    """Subtract warp_ticks (90kHz ticks) from a TS packet's PCR/PTS/DTS, if present.

    warp_ticks is the running total of upstream dead time the dispatcher has
    chosen not to reproduce (see GAP_CAP_SECONDS in dispatch_loop). Shifting
    every clock reference in the stream by the same amount preserves their
    original relative timing (so audio/video stay in sync) while removing the
    stall's real-time footprint, which is what actually causes a demuxer to
    treat a stall/catch-up as a clock discontinuity and produce visible jank.
    """
    if packet[0] != 0x47:
        return

    payload_unit_start = bool(packet[1] & 0x40)
    adaptation_field_control = (packet[3] >> 4) & 0x3
    payload_offset = 4

    if adaptation_field_control in (2, 3):
        af_length = packet[4]
        if af_length > 0:
            flags = packet[5]
            if flags & 0x10 and af_length >= 7:  # PCR_flag
                _rewrite_pcr(packet, 6, warp_ticks)
        payload_offset = 5 + af_length

    if adaptation_field_control in (1, 3) and payload_unit_start:
        if payload_offset + 9 <= TS_PACKET_SIZE and bytes(packet[payload_offset : payload_offset + 3]) == b"\x00\x00\x01":
            pts_dts_flags = (packet[payload_offset + 7] >> 6) & 0x3
            ts_offset = payload_offset + 9
            if pts_dts_flags & 0x2 and ts_offset + 5 <= TS_PACKET_SIZE:
                _rewrite_timestamp(packet, ts_offset, warp_ticks)
                if pts_dts_flags == 0x3 and ts_offset + 10 <= TS_PACKET_SIZE:
                    _rewrite_timestamp(packet, ts_offset + 5, warp_ticks)


def _restamp_chunk(data: bytes, warp_ticks: int) -> bytes:
    if warp_ticks == 0 or len(data) % TS_PACKET_SIZE != 0:
        return data
    packet_bytes = bytearray(data)
    for offset in range(0, len(packet_bytes), TS_PACKET_SIZE):
        _restamp_ts_packet(memoryview(packet_bytes)[offset : offset + TS_PACKET_SIZE], warp_ticks)
    return bytes(packet_bytes)


async def _send_to_clients(clients: list, data: bytes) -> None:
    for writer in list(clients):
        if writer.is_closing():
            clients.remove(writer)
            continue
        try:
            writer.write(data)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            clients.remove(writer)


async def dispatch_loop(queue: asyncio.Queue, clients: list, delay_seconds: float) -> None:
    """Play buffered data out to connected clients, using the buffer as slack
    to absorb upstream gaps rather than reproducing them.

    After an initial ramp-up (waiting for the first item to age past
    delay_seconds, to build up a cushion of backlog), playback stops
    replaying each item's original arrival gap verbatim -- that would just
    reproduce every upstream stall delay_seconds later, which defeats the
    point of buffering. Instead each gap is capped at GAP_CAP_SECONDS, and
    any excess dead time is absorbed by draining the backlog faster. A
    stall shorter than the buffered cushion becomes invisible to clients
    instead of reappearing delay_seconds later; only a stall that empties
    the whole cushion surfaces as a real pause.

    Idles (queue genuinely empty, cushion exhausted) are filled with null
    TS packets (see NULL_TS_PACKET) rather than silence.

    Every skipped gap also accumulates into warp_ticks, which is subtracted
    from each packet's PCR/PTS/DTS before it's sent (see _restamp_chunk) so
    the stream's own embedded clock reflects what we actually deliver rather
    than Blink's original (stall-inclusive) timing.
    """
    primed = False
    last_sent_arrival = None
    warp_ticks = 0

    report_window_start = time.monotonic()
    items_in_window = 0
    bytes_in_window = 0

    while True:
        try:
            arrival_time, data = await asyncio.wait_for(queue.get(), KEEPALIVE_INTERVAL)
        except asyncio.TimeoutError:
            await _send_to_clients(clients, NULL_TS_PACKET)
            continue

        if not primed:
            target_time = arrival_time + delay_seconds
            while (remaining := target_time - time.monotonic()) > 0:
                await asyncio.sleep(min(remaining, KEEPALIVE_INTERVAL))
                if time.monotonic() < target_time:
                    await _send_to_clients(clients, NULL_TS_PACKET)
            primed = True
        elif last_sent_arrival is not None:
            real_gap = max(arrival_time - last_sent_arrival, 0)
            wait = min(real_gap, GAP_CAP_SECONDS)
            skipped = real_gap - wait
            if skipped > 0:
                warp_ticks += round(skipped * PCR_TICKS_PER_SEC)
                log.info(
                    "Dispatch: capped a %.2fs gap to %.2fs (skipped %.2fs, cumulative warp %.2fs) -- visible to OBS now",
                    real_gap,
                    wait,
                    skipped,
                    warp_ticks / PCR_TICKS_PER_SEC,
                )
            else:
                # Healthy transition -- if the cushion has been spent absorbing past stalls,
                # nudge the wait up slightly to slowly rebuild it back toward delay_seconds.
                deficit = delay_seconds - (time.monotonic() - arrival_time)
                if deficit > 0:
                    wait += min(deficit, real_gap * REPLENISH_FRACTION)
            if wait > 0:
                await asyncio.sleep(wait)

        last_sent_arrival = arrival_time
        try:
            data = _restamp_chunk(data, warp_ticks)
        except Exception:
            log.exception("Failed to restamp chunk, sending it unmodified")
        await _send_to_clients(clients, data)

        items_in_window += 1
        bytes_in_window += len(data)
        window_elapsed = time.monotonic() - report_window_start
        if window_elapsed >= THROUGHPUT_REPORT_INTERVAL:
            current_lag = time.monotonic() - arrival_time
            log.info(
                "Dispatch throughput: %.1f items/s, %.0f bytes/s over the last %.1fs "
                "-- currently %.1fs behind live (started at %.1fs)",
                items_in_window / window_elapsed,
                bytes_in_window / window_elapsed,
                window_elapsed,
                current_lag,
                delay_seconds,
            )
            report_window_start = time.monotonic()
            items_in_window = 0
            bytes_in_window = 0


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, clients: list) -> None:
    peer = writer.get_extra_info("peername")
    log.info("Client connected: %s", peer)
    clients.append(writer)
    try:
        while not writer.is_closing():
            if not await reader.read(1024):
                break
    except (ConnectionResetError, OSError):
        pass
    finally:
        if writer in clients:
            clients.remove(writer)
        if not writer.is_closing():
            writer.close()
        log.info("Client disconnected: %s", peer)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("camera_name", help='Camera name exactly as shown in the Blink app, e.g. "Front Door"')
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind the stream server to (0.0.0.0 = reachable from other machines on the LAN)",
    )
    parser.add_argument("--port", type=int, default=8554, help="Local port to bind the stream server to")
    parser.add_argument(
        "--buffer-seconds",
        type=float,
        default=20,
        help="Seconds to delay playback by, to absorb upstream reconnect gaps (default: 20)",
    )
    args = parser.parse_args()

    async with ClientSession() as session:
        blink = await login(session)

        if args.camera_name not in blink.cameras:
            available = ", ".join(blink.cameras.keys())
            raise SystemExit(f'Camera "{args.camera_name}" not found. Available cameras: {available}')
        camera = blink.cameras[args.camera_name]

        clients: list = []
        queue: asyncio.Queue = asyncio.Queue()

        server = await asyncio.start_server(lambda r, w: handle_client(r, w, clients), args.host, args.port)
        sockname = server.sockets[0].getsockname()
        display_host = lan_ip() if sockname[0] in ("0.0.0.0", "::") else sockname[0]
        log.info(
            "Streaming (buffered %ss behind live) at tcp://%s:%s (point OBS/ffplay here)",
            args.buffer_seconds,
            display_host,
            sockname[1],
        )

        async with server:
            await asyncio.gather(
                ingest_loop(camera, args.camera_name, queue),
                dispatch_loop(queue, clients, args.buffer_seconds),
                server.serve_forever(),
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
