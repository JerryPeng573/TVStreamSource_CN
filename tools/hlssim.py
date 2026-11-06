#!/usr/bin/env python3
"""Virtual HLS player: plays a stream for N seconds the way ExoPlayer does and reports the stalls.

mytv-android plays through Media3/ExoPlayer with its defaults, so this reproduces the parts of that
behaviour which decide whether a viewer sees a stall:

  * User-Agent "ExoPlayer"
  * starts about three target durations behind the live edge
  * can only buffer what the playlist has published, so the buffer is capped by the distance to the edge
  * starts playing after 2.5 s buffered, resumes after a stall once 5 s is buffered

Nothing is decoded. Downloading is real, and playback is a clock that drains the buffer, which is
enough to count stalls. It is used to compare a direct line with the gateway on equal terms:

    python tools/hlssim.py http://example/live.m3u8 --secs 120
"""
import argparse
import json
import sys
import threading
import time
from urllib.parse import urljoin

import http.client
from urllib.parse import urlsplit

UA = "ExoPlayer"
START_BUFFER = 2.5  # DefaultLoadControl.DEFAULT_BUFFER_FOR_PLAYBACK_MS
REBUFFER = 5.0  # DefaultLoadControl.DEFAULT_BUFFER_FOR_PLAYBACK_AFTER_REBUFFER_MS
LOAD_TIMEOUT = 15.0  # Constants.VIDEO_PLAYER_LOAD_TIMEOUT
SOCKET_TIMEOUT = 15.0


def http_get(url, timeout=SOCKET_TIMEOUT, redirects=4, on_chunk=None):
    """GET a URL with the player's User-Agent. Returns (status, body, final_url). on_chunk(bytes,
    total_or_None) sees a 200 response as it arrives."""
    for _ in range(redirects + 1):
        u = urlsplit(url)
        cls = http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection
        conn = cls(u.hostname, u.port, timeout=timeout)
        try:
            conn.request("GET", (u.path or "/") + ("?" + u.query if u.query else ""),
                         headers={"User-Agent": UA, "Accept": "*/*", "Connection": "close"})
            r = conn.getresponse()
            if r.status in (301, 302, 303, 307, 308) and r.getheader("Location"):
                url = urljoin(url, r.getheader("Location"))
                continue
            if not on_chunk or r.status != 200:
                return r.status, r.read(), url
            total = int(r.getheader("Content-Length") or 0) or None
            body = bytearray()
            while True:
                chunk = r.read1(65536)
                if not chunk:
                    return r.status, bytes(body), url
                body += chunk
                on_chunk(chunk, total)
        finally:
            conn.close()
    return 0, b"", url


def parse_media(text, base):
    """-> dict(seq, target, segs=[(seq, dur, url, disc)], ended)"""
    seq, target, ended, disc = 0, 0.0, False, False
    segs, dur = [], 0.0
    n = None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            seq = int(line.split(":")[1])
        elif line.startswith("#EXT-X-TARGETDURATION:"):
            target = float(line.split(":")[1])
        elif line.startswith("#EXT-X-ENDLIST"):
            ended = True
        elif line.startswith("#EXT-X-DISCONTINUITY") and not line.startswith("#EXT-X-DISCONTINUITY-SEQUENCE"):
            disc = True
        elif line.startswith("#EXTINF:"):
            try:
                dur = float(line[8:].split(",")[0])
            except ValueError:
                dur = 0.0
        elif line and not line.startswith("#"):
            n = seq + len(segs)
            segs.append((n, dur, urljoin(base, line), disc))
            disc = False
    return {"seq": seq, "target": target or 6.0, "segs": segs, "ended": ended}


def pick_variant(text, base, max_bps=8_000_000):
    """Highest variant under max_bps, roughly what ExoPlayer's bandwidth meter settles on."""
    lines = text.splitlines()
    variants = []
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            bw = 0
            for part in line.split(":", 1)[1].replace('"', "").split(","):
                if part.startswith("BANDWIDTH="):
                    bw = int(part[10:])
            for nxt in lines[i + 1:]:
                if nxt.strip() and not nxt.startswith("#"):
                    variants.append((bw, urljoin(base, nxt.strip())))
                    break
    if not variants:
        return None
    under = [v for v in variants if v[0] <= max_bps]
    return max(under or [min(variants)], key=lambda v: v[0])[1]


def simulate(url, secs=120, log=None):
    """Play url for secs wall-clock seconds and return the viewer-side result."""
    res = {"url": url, "startup": None, "stalls": 0, "stall_secs": 0.0, "failed": None, "segments": 0,
           "bytes": 0, "played": 0.0, "discontinuities": 0, "behind_live": 0, "min_buffer": None,
           "codec_switch": 0}
    t0 = time.time()
    say = log or (lambda *_: None)
    state = {"buffered": 0.0, "stop": False, "err": None, "last_load": t0}
    lock = threading.Lock()

    def loader():
        try:
            status, body, base = http_get(url)
            if status != 200 or not body.lstrip().startswith(b"#EXTM3U"):
                state["err"] = f"playlist http {status}"
                return
            text = body.decode("utf-8", "replace")
            media_url = url
            if "#EXT-X-STREAM-INF" in text:
                media_url = pick_variant(text, base)
                status, body, base = http_get(media_url)
                if status != 200:
                    state["err"] = f"variant http {status}"
                    return
                text = body.decode("utf-8", "replace")
            pl = parse_media(text, base)
            if not pl["segs"]:
                state["err"] = "no segments"
                return
            next_seq = pl["segs"][max(0, len(pl["segs"]) - 3)][0]
            last_reload = time.time()
            stuck_since = time.time()
            last_edge = pl["segs"][-1][0]
            while not state["stop"]:
                pending = [s for s in pl["segs"] if s[0] >= next_seq]
                if pl["segs"] and next_seq < pl["segs"][0][0]:
                    res["behind_live"] += 1  # ExoPlayer: BehindLiveWindowException, seek to the default position
                    next_seq = pl["segs"][max(0, len(pl["segs"]) - 3)][0]
                    say("behind live window")
                    continue
                if pending:
                    n, dur, seg_url, disc = pending[0]
                    if disc:
                        res["discontinuities"] += 1
                    got = [0, 0.0]  # bytes, seconds credited

                    def credit(chunk, total):
                        got[0] += len(chunk)
                        # without a length (segment still arriving) assume a typical 3.2 Mbps
                        part = dur * got[0] / total if total else min(dur, got[0] * 8 / 3_200_000)
                        with lock:
                            state["buffered"] += part - got[1]
                        got[1] = part

                    status, data, _ = http_get(seg_url, on_chunk=credit)
                    if status != 200:
                        state["err"] = f"segment http {status}"
                        return
                    with lock:
                        state["buffered"] += dur - got[1]
                    state["last_load"] = time.time()
                    res["segments"] += 1
                    res["bytes"] += len(data)
                    next_seq = n + 1
                    continue
                wait = max(0.2, pl["target"] / 2 - (time.time() - last_reload))
                time.sleep(wait)
                status, body, base = http_get(media_url)
                last_reload = time.time()
                if status != 200:
                    state["err"] = f"reload http {status}"
                    return
                pl = parse_media(body.decode("utf-8", "replace"), base)
                edge = pl["segs"][-1][0] if pl["segs"] else last_edge
                if edge != last_edge:
                    last_edge, stuck_since = edge, time.time()
                elif time.time() - stuck_since > 3.5 * pl["target"]:  # ExoPlayer: PlaylistStuckException
                    state["err"] = "playlist stuck"
                    return
        except Exception as e:
            state["err"] = f"{type(e).__name__}: {e}"

    th = threading.Thread(target=loader, daemon=True)
    th.start()

    playing, rebuffering, stall_t = False, False, 0.0
    tick = 0.05
    last = time.time()
    while time.time() - t0 < secs:
        time.sleep(tick)
        now = time.time()
        dt = now - last
        last = now
        with lock:
            buf = state["buffered"]
            if playing:
                if res["min_buffer"] is None or buf < res["min_buffer"]:
                    res["min_buffer"] = round(buf, 1)
                if buf <= 0:
                    playing, rebuffering = False, True
                    stall_t = now
                    res["stalls"] += 1
                    say(f"stall #{res['stalls']} at {now - t0:.1f}s")
                else:
                    state["buffered"] = max(0.0, buf - dt)
                    res["played"] += dt
            else:
                need = REBUFFER if rebuffering else START_BUFFER
                if buf >= need:
                    if rebuffering:
                        res["stall_secs"] += now - stall_t
                    else:
                        res["startup"] = round(now - t0, 2)
                        say(f"playing after {res['startup']}s")
                    playing, rebuffering = True, False
                elif not rebuffering and res["startup"] is None and now - t0 > LOAD_TIMEOUT:
                    res["failed"] = "load timeout"
                    break
                elif rebuffering and now - stall_t > LOAD_TIMEOUT:
                    res["stall_secs"] += now - stall_t
                    res["failed"] = "cutoff"  # mytv-android reloads the same line here
                    break
        if state["err"] and state["buffered"] <= 0:
            res["failed"] = state["err"]
            break
    state["stop"] = True
    if rebuffering and not res["failed"]:
        res["stall_secs"] += time.time() - stall_t
    res["stall_secs"] = round(res["stall_secs"], 1)
    res["played"] = round(res["played"], 1)
    res["mbps"] = round(res["bytes"] * 8 / max(res["played"], 1) / 1e6, 2)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--secs", type=int, default=120)
    a = ap.parse_args()
    print(json.dumps(simulate(a.url, a.secs, log=lambda m: print(" ", m, flush=True)), indent=1))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
