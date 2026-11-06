#!/usr/bin/env python3
"""Home HLS gateway: one steady LAN stream per channel, built from unreliable public lines.

Why this exists. mytv-android (like most IPTV players) can only react once a stream has failed, and
it never leaves a line for being slow: a stall reloads the same line, and the last line in a list is
never retried. So however good a static playlist is on the day it is built, a viewer still sees
every failure. This gateway moves those decisions to an always-on machine on the home network:

    upstream lines  --(race, health ranking, failover)-->  gateway buffer  --LAN-->  stock player

The player sees a single ordinary live HLS stream that never errors. Behind it the gateway

  * races several lines at tune-in and keeps the first that really delivers video,
  * sits some seconds behind the upstream live edge, so a failing line is replaced before the
    buffer runs out, and the viewer does not see it happen,
  * follows the new line from the same point in the broadcast, marked with EXT-X-DISCONTINUITY,
  * pins where a line redirects to, and refuses advert streams (see "Redirects" below),
  * remembers per-line health, so the next tune-in starts on a line that has been behaving,
  * shows a "no signal" slate rather than an error if every line is down, and recovers by itself.

Redirects. Most public relay URLs are redirectors that choose a destination on every request: often
another mirror, sometimes a TV-shopping advert loop served in place of the channel. A player rereads
the original URL at every playlist refresh, so it rolls that dice every few seconds and can land on
a different stream mid-programme, which shows up as stalls, jumps, errors, or an advert. The gateway
rolls until it gets a real stream, then keeps reading that exact address.

Only channels being watched are pulled. Python standard library only.

    python3 gateway/iptv_gateway.py --lines gateway/lines.json --state ~/.local/state/iptv-gateway

    GET /tv.m3u                playlist for the player
    GET /live/<channel>.m3u8   live stream (session starts on the first request)
    GET /status                what each session is doing
    GET /switch/<channel>      leave the current line for another one
    GET /                      channel list and a browser player, for checking from any device
"""
import argparse
import http.client
import json
import logging
import os
import queue
import random
import socket
import ssl
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlsplit

log = logging.getLogger("gateway")
HLS_JS = Path(__file__).resolve().parent.parent / "player" / "hls.min.js"

UA = "ExoPlayer"  # what the player would send, and what lines were tested with

# ---- tuning -----------------------------------------------------------------------------------
BUFFER_SECS = 30.0  # how far behind the upstream edge to sit; capped by the upstream window
START_MEDIA = 4.0  # seconds of media to hold before answering the first playlist request
FIRST_WAIT = 11.0  # the player gives up at 15 s, so show the slate before that
RACE_WIDTH = 3  # lines tried at once
RACE_DEADLINE = 7.0
RACE_GRACE = 0.35  # after the first winner, wait this long for a faster one
VALIDATE_BYTES = 64_000  # this much real MPEG-TS proves a line works
MAX_BPS = 12_000_000  # ignore variants above this; the home link cannot carry them reliably
CONNECT_TIMEOUT = 3.0
READ_TIMEOUT = 5.0
RETAIN_SECS = 120.0  # segments kept for serving
PLAYLIST_SECS = 60.0  # media listed in the playlist
MAX_SESSIONS = 3
IDLE_SECS = 20.0  # stop pulling a channel this long after the last request
SLOW_RT = 1.2  # below this realtime factor a line is not keeping up
COOLDOWN_SECS = 180.0  # a line that failed is not tried again for this long, unless nothing else works
TICK = 0.2
MONITOR_GAP = 25.0  # seconds between background line checks
MONITOR_BYTES = 300_000  # sample this much of a segment, enough to see speed without wasting data
TYPICAL_BPS = 3_300_000  # median bitrate measured across the lines; turns a speed into a realtime factor
SYNC = 0x47


# Stream addresses known to be advert loops, not channels: relays redirect to these in place of the
# channel. tools/build_playlist.py adds any others it detects, through the "decoys" list in lines.json.
KNOWN_DECOYS = {"/applive/mkt.m3u8", "/applive/yss.m3u8", "/applive/102.m3u8"}
DECOY_ROLLS = 4  # a redirector may hand out an advert on one request and the channel on the next
decoys = set(KNOWN_DECOYS)


def stream_keys(url):
    """The ways a stream can be listed as a decoy: by path, or by server and path."""
    u = urlsplit(url)
    return u.path, f"{u.hostname}:{u.port or 80}{u.path}"


def is_decoy(url):
    return any(k in decoys for k in stream_keys(url))


class LineError(Exception):
    """A line stopped working. The message is the reason, recorded in the health data."""


class Decoy(LineError):
    """The line led to an advert loop on this request. It may lead to the channel on the next."""


# ---- HTTP client ------------------------------------------------------------------------------
class Fetcher:
    """Small keep-alive HTTP client. Reusing connections matters on long paths: a new TCP connection
    for each segment restarts slow start, and at 200 ms round trip that costs seconds."""

    def __init__(self):
        self.conns = {}

    def close(self):
        for c in self.conns.values():
            c.close()
        self.conns.clear()

    def _connection(self, u):
        key = (u.scheme, u.hostname, u.port)
        conn = self.conns.pop(key, None)
        if conn is not None:
            return key, conn, False
        if u.scheme == "https":
            conn = http.client.HTTPSConnection(u.hostname, u.port, timeout=CONNECT_TIMEOUT,
                                               context=ssl.create_default_context())
        else:
            conn = http.client.HTTPConnection(u.hostname, u.port, timeout=CONNECT_TIMEOUT)
        conn.connect()
        conn.sock.settimeout(READ_TIMEOUT)
        return key, conn, True

    def get(self, url, deadline_s, max_bytes=64_000_000, cancel=None, sample_bytes=None, on_chunk=None):
        """Returns (status, body, final_url, ttfb_s). Raises LineError on network failure or deadline.
        on_chunk(bytes) sees a 200 response as it arrives, and may raise LineError to abandon it."""
        started = time.monotonic()
        deadline = started + deadline_s
        for _ in range(5):
            u = urlsplit(url)
            path = (u.path or "/") + ("?" + u.query if u.query else "")
            headers = {"User-Agent": UA, "Accept": "*/*"}
            r, conn, key = None, None, None
            for attempt in (1, 2):  # a keep-alive connection may have been closed by the server
                try:
                    key, conn, fresh = self._connection(u)
                    conn.request("GET", path, headers=headers)
                    r = conn.getresponse()
                    break
                except (http.client.HTTPException, OSError) as e:
                    if conn is not None:
                        conn.close()
                    conn = None
                    if attempt == 2 or (key is not None and fresh):
                        raise LineError(f"connect: {type(e).__name__}") from None
            ttfb = time.monotonic() - started
            if r.status in (301, 302, 303, 307, 308):
                location = r.getheader("Location")
                r.read()
                self._release(key, conn, r)
                if not location:
                    raise LineError("redirect without location")
                url = urljoin(url, location)
                continue
            buf = bytearray()
            try:
                while True:
                    chunk = r.read1(65536)
                    if not chunk:
                        break
                    buf += chunk
                    if on_chunk and r.status == 200:
                        try:
                            on_chunk(chunk)
                        except LineError:
                            conn.close()
                            raise
                    if cancel and cancel():
                        conn.close()
                        raise LineError("cancelled")
                    if sample_bytes and len(buf) >= sample_bytes:
                        conn.close()
                        return r.status, bytes(buf), url, ttfb
                    if len(buf) > max_bytes:
                        conn.close()
                        raise LineError("response too large")
                    if time.monotonic() > deadline:
                        conn.close()
                        raise LineError("deadline")
            except (http.client.HTTPException, OSError) as e:
                conn.close()
                raise LineError(f"read: {type(e).__name__}") from None
            self._release(key, conn, r)
            return r.status, bytes(buf), url, ttfb
        raise LineError("too many redirects")

    def _release(self, key, conn, resp):
        if resp.will_close:
            conn.close()
        else:
            self.conns[key] = conn


# ---- HLS parsing ------------------------------------------------------------------------------
class Playlist:
    def __init__(self, text, base):
        self.base, self.target, self.seq = base, 0.0, 0
        self.has_seq = self.ended = self.encrypted = self.fmp4 = False
        self.segs = []  # (seq, duration, url)
        duration = 0.0
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("#EXT-X-TARGETDURATION:"):
                self.target = float(line.split(":", 1)[1])
            elif line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                self.seq, self.has_seq = int(line.split(":", 1)[1]), True
            elif line.startswith("#EXT-X-ENDLIST"):
                self.ended = True
            elif line.startswith("#EXT-X-KEY") and "METHOD=NONE" not in line:
                self.encrypted = True
            elif line.startswith("#EXT-X-MAP"):
                self.fmp4 = True
            elif line.startswith("#EXTINF:"):
                try:
                    duration = float(line[8:].split(",")[0])
                except ValueError:
                    duration = 0.0
            elif line and not line.startswith("#"):
                self.segs.append((self.seq + len(self.segs), duration or self.target or 6.0, urljoin(base, line)))
        if not self.target and self.segs:
            self.target = max(s[1] for s in self.segs)


def pick_variant(text, base):
    lines = text.splitlines()
    variants = []
    for i, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF"):
            continue
        bw = 0
        for part in line.split(":", 1)[1].replace('"', "").split(","):
            if part.startswith("BANDWIDTH="):
                try:
                    bw = int(part[10:])
                except ValueError:
                    pass
        for nxt in lines[i + 1:]:
            if nxt.strip() and not nxt.startswith("#"):
                variants.append((bw, urljoin(base, nxt.strip())))
                break
    if not variants:
        return None
    under = [v for v in variants if v[0] <= MAX_BPS]
    return max(under or [min(variants)], key=lambda v: v[0])[1]


def looks_like_ts(data):
    if len(data) < 188 * 3:
        return False
    for off in range(0, min(len(data) - 376, 1024)):
        if data[off] == SYNC and data[off + 188] == SYNC and data[off + 376] == SYNC:
            return True
    return False


def start_index(pl, behind):
    """Index of the segment about `behind` seconds behind the live edge."""
    total, idx = 0.0, len(pl.segs)
    while idx > 0 and total < behind:
        idx -= 1
        total += pl.segs[idx][1]
    return min(idx, len(pl.segs) - 1)


class Line:
    """One upstream line as a session is following it."""

    def __init__(self, url):
        self.url = url
        self.media_url = self.pinned = url
        self.relocated = False
        self.fetch = Fetcher()
        self.last_seq = None
        self.last_uri = None
        self.seen = deque(maxlen=128)
        self.uri_mode = False
        self.last_new = time.monotonic()
        self.rt = None
        self.slow_runs = 0

    def load(self, deadline=5.0, cancel=None):
        """Latest media playlist. Reads the pinned address; if that stops answering (tokens in these
        addresses expire), asks the original URL where to go now."""
        if self.media_url == self.url:
            return self._resolve(deadline, cancel)
        try:
            return self._read(self.media_url, deadline, cancel)
        except LineError as e:
            if str(e) == "cancelled":
                raise
            self.media_url = self.url
            pl = self._resolve(deadline, cancel)
            self.relocated = True  # a different server: its sequence numbers mean nothing to us
            return pl

    def _resolve(self, deadline, cancel):
        for _ in range(DECOY_ROLLS):
            try:
                pl = self._read(self.url, deadline, cancel)
                self.media_url = self.pinned  # keep reading this exact stream from now on
                return pl
            except Decoy:
                continue
        raise LineError("advert stream")

    def _read(self, url, deadline, cancel, nested=False):
        status, body, final, _ = self.fetch.get(url, deadline, 2_000_000, cancel)
        if is_decoy(final):
            raise Decoy("advert stream")
        if status != 200:
            raise LineError(f"playlist http {status}")
        if not body.lstrip().startswith(b"#EXTM3U"):
            raise LineError("not a playlist")
        text = body.decode("utf-8", "replace")
        if "#EXT-X-STREAM-INF" in text:
            variant = pick_variant(text, final)
            if not variant or nested:
                raise LineError("unusable master playlist")
            return self._read(variant, deadline, cancel, nested=True)
        self.pinned = final
        try:
            pl = Playlist(text, final)
        except ValueError:
            raise LineError("malformed playlist") from None
        if pl.ended:
            raise LineError("not live")
        if pl.encrypted or pl.fmp4:
            raise LineError("unsupported format")
        if not pl.segs:
            raise LineError("empty playlist")
        return pl

    def new_segments(self, pl):
        """Segments not yet taken. Returns (segments, restarted)."""
        restarted = False
        if self.relocated:  # join the new server near its edge, and tell the player
            self.relocated = False
            self.seen.clear()
            self.uri_mode = False
            self.last_seq = pl.segs[max(0, len(pl.segs) - 3)][0] - 1
            return [s for s in pl.segs if s[0] > self.last_seq], True
        if self.last_seq is not None and not self.uri_mode:
            edge = pl.segs[-1]
            # no usable sequence numbers, or the sequence does not move while the URIs do
            if not pl.has_seq or (edge[0] == self.last_seq and edge[2] != self.last_uri
                                  and edge[2] not in self.seen):
                self.uri_mode = True
        if self.uri_mode:
            new = [s for s in pl.segs if s[2] not in self.seen]
        else:
            if pl.segs[-1][0] < self.last_seq - 2:  # upstream restarted its numbering
                self.last_seq, restarted = pl.seq - 1, True
            new = [s for s in pl.segs if s[0] > self.last_seq]
        return new, restarted

    def mark(self, seg):
        self.last_seq, self.last_uri = seg[0], seg[2]
        self.seen.append(seg[2])
        self.last_new = time.monotonic()


# ---- health -----------------------------------------------------------------------------------
class Health:
    """Per-line memory, kept on disk, so a tune-in starts from lines that have been behaving."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {}
        self.dirty = 0
        if path and path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except ValueError:
                log.warning("health file unreadable, starting empty")

    def note(self, url, ok, rt=None, why=None, weight=0.25):
        now = time.time()
        with self.lock:
            h = self.data.setdefault(url, {"ok": 0.5, "rt": 0.0, "fails": 0, "n": 0})
            h["ok"] = round(h["ok"] * (1 - weight) + (1.0 if ok else 0.0) * weight, 4)
            h["n"] += 1
            h["t"] = int(now)
            if ok:
                h["fails"] = 0
                h["last_ok"] = int(now)
            else:
                h["fails"] += 1
                h["why"] = why
            if rt:
                h["rt"] = round(h["rt"] * 0.7 + min(rt, 20.0) * 0.3, 2) if h["rt"] else round(min(rt, 20.0), 2)
            self.dirty += 1

    def score(self, url):
        h = self.data.get(url)
        if not h:
            return 50.0  # untested: ahead of known-bad lines, behind known-good ones
        return h["ok"] * 100 - min(h["fails"], 5) * 12 + min(h.get("rt", 0.0), 5.0) * 4

    def rank(self, urls):
        return sorted(urls, key=self.score, reverse=True)

    def save(self):
        with self.lock:
            if not self.dirty or not self.path:
                return
            self.dirty = 0
            text = json.dumps(self.data, ensure_ascii=False)
        tmp = self.path.with_suffix(".tmp")
        try:
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(self.path)
        except OSError as e:  # a full disk must not stop the television
            log.error("cannot save health data: %s", e)


# ---- racing -----------------------------------------------------------------------------------
class Racer(threading.Thread):
    """Opens one line and downloads one real segment. The segment fills in as it arrives, so the
    winner can be on screen before its first segment has finished downloading."""

    def __init__(self, url, behind, results, health, cond, stop):
        super().__init__(daemon=True, name="racer")
        self.url, self.behind, self.results, self.health, self.cond = url, behind, results, health, cond
        self.stop_session = stop
        self.lost = threading.Event()
        self.line = Line(url)
        self.pl = self.info = None
        self.seg = Seg(0.0, bytearray(), done=False)
        self.announced = False
        self.rt = 0.0
        self.failed = False

    def cancelled(self):
        return self.lost.is_set() or self.stop_session()

    def _feed(self, chunk):
        with self.cond:
            self.seg.data += chunk
            self.cond.notify_all()
        if not self.announced and len(self.seg.data) >= VALIDATE_BYTES:
            self._announce()

    def _announce(self):
        if not looks_like_ts(bytes(self.seg.data[:4096])):
            raise LineError("segment is not MPEG-TS")
        self.announced = True
        self.results.put(self)

    def run(self):
        try:
            self.pl = self.line.load(5.0, self.cancelled)
            idx = start_index(self.pl, self.behind)
            for attempt in (idx, idx + 1):  # the first choice can expire while we ask for it
                if attempt >= len(self.pl.segs):
                    break
                self.info = self.pl.segs[attempt]
                self.seg.dur = self.info[1]
                t0 = time.monotonic()
                status, _, _, _ = self.line.fetch.get(self.info[2], max(RACE_DEADLINE, self.info[1] * 2.5),
                                                      cancel=self.cancelled, on_chunk=self._feed)
                if status == 200:
                    if not self.announced:
                        self._announce()
                    self.rt = self.info[1] / max(time.monotonic() - t0, 0.01)
                    self.health.note(self.url, True, rt=self.rt)
                    return
            raise LineError("segment unavailable")
        except LineError as e:
            self._fail(str(e))
        except Exception as e:  # a malformed playlist must never take a session down
            self._fail(type(e).__name__)
        finally:
            with self.cond:
                self.seg.done = True
                self.cond.notify_all()

    def _fail(self, why):
        self.failed = True
        self.line.fetch.close()
        if why != "cancelled":
            self.health.note(self.url, False, why=why)
        if not self.announced:
            self.results.put(None)


def race(urls, behind, health, cond, stop):
    """Try lines at once. Returns the running Racer of the best one, or None. A win means the line
    has delivered real video; the winner carries on downloading its segment in the background."""
    results = queue.Queue()
    racers = [Racer(u, behind, results, health, cond, stop) for u in urls]
    for r in racers:
        r.start()
    winners, finished = [], 0
    limit = time.monotonic() + RACE_DEADLINE
    grace_end = None
    while finished < len(urls) and not stop():
        wait = limit - time.monotonic() if grace_end is None else grace_end - time.monotonic()
        if wait <= 0:
            break
        try:
            item = results.get(timeout=min(wait, 0.5))
        except queue.Empty:
            continue
        finished += 1
        if item:
            winners.append(item)
            if grace_end is None:
                grace_end = time.monotonic() + RACE_GRACE
    if not winners or stop():
        for r in racers:
            r.lost.set()
        return None
    best = max(winners, key=lambda r: len(r.seg.data))  # they all started together
    for r in racers:
        if r is not best:
            r.lost.set()
    return best


# ---- session ----------------------------------------------------------------------------------
class Seg:
    __slots__ = ("n", "dur", "data", "disc", "slate", "t", "done")

    def __init__(self, dur, data, disc=False, slate=False, done=True):
        self.n, self.dur, self.data, self.disc, self.slate, self.t = 0, dur, data, disc, slate, 0.0
        self.done = done


class Session:
    def __init__(self, gw, channel):
        self.gw, self.channel = gw, channel
        self.id = f"{random.getrandbits(32):08x}"
        self.created = self.last_seen = time.monotonic()
        self.cond = threading.Condition()
        self.pending = deque()  # downloaded, not yet in the playlist
        self.published = deque()  # in the playlist
        self.next_n = 1
        self.disc_seq = 0
        self.t0 = None  # playout clock start
        self.last_add = time.monotonic()
        self.pub_media = 0.0
        self.target = 0.0
        self.stopped = False
        self.line = None
        self.cool = {}
        self.need_disc = False
        self.behind = BUFFER_SECS
        self.leave = False  # set by /switch: drop this line at the next chance
        self.stats = {"switches": 0, "slate_secs": 0.0, "history": []}
        threading.Thread(target=self._puller, daemon=True, name=f"pull-{channel['id']}").start()
        threading.Thread(target=self._clock, daemon=True, name=f"clock-{channel['id']}").start()

    def touch(self):
        self.last_seen = time.monotonic()

    def stop(self):
        self.stopped = True
        with self.cond:
            self.cond.notify_all()

    # -- line choice
    def _candidates(self):
        """Groups of lines to try in turn. A line that just failed must not race to replace itself:
        a frozen line still serves a perfectly good old segment, so it could win."""
        now = time.monotonic()
        ranked = self.gw.health.rank(self.channel["lines"])
        return ([u for u in ranked if self.cool.get(u, 0) <= now],
                [u for u in ranked if self.cool.get(u, 0) > now])

    def _acquire(self):
        """Find a working line, going round every line until one answers."""
        behind = max(self.behind, 0.0)
        while not self.stopped:
            for order in self._candidates():
                for i in range(0, len(order), RACE_WIDTH):
                    won = race(order[i:i + RACE_WIDTH], behind, self.gw.health, self.cond,
                               lambda: self.stopped)
                    if won:
                        return won
                    if self.stopped:
                        return None
            log.warning("%s: no line answered, retrying", self.channel["id"])
            for _ in range(int(3 / TICK)):
                if self.stopped:
                    return None
                time.sleep(TICK)
        return None

    def _switch(self, why):
        line = self.line
        self.stats["switches"] += 1
        self.stats["history"].append({"t": int(time.time()), "from": line.url, "why": why})
        del self.stats["history"][:-20]
        log.info("%s: leaving %s (%s), %.1fs cushion", self.channel["id"], line.url, why, self.cushion())
        self.cool[line.url] = time.monotonic() + COOLDOWN_SECS
        self.gw.health.note(line.url, False, why=why)
        line.fetch.close()
        # Pick the broadcast up where this line left it, so the buffer refills and nothing is skipped.
        self.behind = min(BUFFER_SECS, self.behind + (time.monotonic() - line.last_new))
        self.line = None
        self.need_disc = True

    def cushion(self):
        """Seconds of media ahead of the viewer: what is left if the line stops now."""
        with self.cond:
            ahead = self.pub_media - (time.monotonic() - self.t0) if self.t0 is not None else 0.0
            return max(ahead, 0.0) + sum(s.dur for s in self.pending)

    def _add(self, seg):
        seg.disc = seg.disc or self.need_disc
        self.need_disc = False
        with self.cond:
            self.pending.append(seg)
            self.last_add = time.monotonic()
            self.cond.notify_all()

    # -- following a line
    def _puller(self):
        try:
            while not self.stopped:
                if self.line is None:
                    won = self._acquire()
                    if won is None:
                        return
                    self.target = max(self.target, won.pl.target)
                    self._add(won.seg)  # may still be arriving, and is served as it does
                    won.join()
                    self.line = won.line
                    if won.failed:  # cut short: keep what came, and tell the player the timeline jumps
                        self.need_disc = True
                    self.line.rt = won.rt or None
                    self.line.mark(won.info)
                    for s in won.pl.segs:  # with no sequence numbers, older URIs must count as seen
                        if s[0] <= won.info[0]:
                            self.line.seen.append(s[2])
                    log.info("%s: on %s (rt %.1f)", self.channel["id"], self.line.url, won.rt)
                try:
                    self._follow()
                except LineError as e:
                    if str(e) != "cancelled":
                        self._switch(str(e))
        except Exception:
            log.exception("%s: puller crashed", self.channel["id"])
            self.gw.drop(self)
        finally:
            if self.line:
                self.line.fetch.close()

    def _follow(self):
        line = self.line
        misses = 0
        while not self.stopped:
            try:
                pl = line.load(5.0, lambda: self.stopped)
                misses = 0
            except LineError as e:
                misses += 1  # one bad poll is noise while there is a cushion, two is a failing line
                if misses >= 2 or str(e) == "cancelled" or self.cushion() < 6:
                    raise
                self._sleep(1.0)
                continue
            self.target = max(self.target, pl.target)
            if self.leave:
                self.leave = False
                raise LineError("switched by hand")
            new, restarted = line.new_segments(pl)
            if restarted:
                self.need_disc = True
            if not new:
                # Upstream windows are often only 30 s, so this must be decided well inside that.
                if time.monotonic() - line.last_new > max(1.5 * (pl.target or 6), 6):
                    raise LineError("stale playlist")
                self._sleep(min(max(pl.target / 2, 0.5), 3.0))
                continue
            for i, seg in enumerate(new):
                if self.stopped:
                    return
                if not line.uri_mode and seg[0] != line.last_seq + 1:
                    self.need_disc = True  # lost segments: tell the player the timeline jumps
                ahead = sum(s[1] for s in new[i + 1:])
                self.behind = ahead + seg[1]
                self._download(line, seg)

    def _download(self, line, seg):
        # A segment may take as long as the cushion can cover, while leaving time for a race.
        deadline = max(3.0, min(self.cushion() * 0.5 + seg[1] * 0.5, 20.0))
        for attempt in (1, 2):
            t0 = time.monotonic()
            try:
                status, data, _, _ = line.fetch.get(seg[2], deadline, cancel=lambda: self.stopped)
                break
            except LineError as e:
                if attempt == 2 or str(e) in ("deadline", "cancelled") or self.cushion() < 6:
                    raise
        if status != 200:
            raise LineError(f"segment http {status}")
        if not looks_like_ts(data):
            raise LineError("segment is not MPEG-TS")
        elapsed = max(time.monotonic() - t0, 0.01)
        rt = seg[1] / elapsed
        line.rt = rt if line.rt is None else line.rt * 0.6 + rt * 0.4
        line.mark(seg)
        self._add(Seg(seg[1], data))
        self.gw.health.note(line.url, True, rt=rt, weight=0.05)
        # Being slow is not an error to the player, so it has to be judged here.
        line.slow_runs = line.slow_runs + 1 if line.rt < SLOW_RT else 0
        if line.slow_runs >= 3 and self.cushion() < BUFFER_SECS / 2 and len(self.channel["lines"]) > 1:
            raise LineError(f"slow (rt {line.rt:.2f})")

    def _sleep(self, secs):
        end = time.monotonic() + secs
        while not self.stopped and time.monotonic() < end:
            time.sleep(min(TICK, end - time.monotonic()))

    # -- playout clock: decides what the player may see
    def _lead(self):
        t = self.target or 6.0
        depth = sum(s.dur for s in self.pending)
        return 3 * t + 1 + max(0.0, depth - BUFFER_SECS - 2 * t)  # drain any excess that builds up

    def _publish(self, seg):
        if self.published and self.published[-1].slate and not seg.slate:
            seg.disc = True
        seg.n = self.next_n
        seg.t = time.monotonic()
        self.next_n += 1
        self.published.append(seg)
        self.pub_media += seg.dur

    def _clock(self):
        try:
            self._run_clock()
        except Exception:
            log.exception("%s: clock crashed", self.channel["id"])
            self.gw.drop(self)

    def _run_clock(self):
        while not self.stopped:
            time.sleep(TICK)
            now = time.monotonic()
            with self.cond:
                changed = False
                if self.t0 is None:
                    ready = sum(s.dur for s in self.pending)
                    waited = now - self.created
                    if ready >= START_MEDIA or (self.pending and waited > 3.0):
                        self.t0 = now
                    elif waited > FIRST_WAIT:
                        self.t0 = now  # nothing yet: start the slate so the player does not give up
                if self.t0 is None:
                    continue
                pos = now - self.t0
                while self.pending and self.pub_media - pos < self._lead():
                    self._publish(self.pending.popleft())
                    changed = True
                t = self.target or 6.0
                if not self.pending and self.gw.slate and now - self.last_add > 1.2 * t + 2:
                    # Nothing has arrived for longer than a segment takes to appear. The player
                    # rereads the playlist every half target duration or so, and the slate has to
                    # be listed before its buffer runs out.
                    keep = min(self._lead(), 0.6 * t + 1.5)
                    while self.pub_media - pos < keep:
                        slate = Seg(self.gw.slate_dur, self.gw.slate, disc=True, slate=True)
                        self._publish(slate)
                        self.stats["slate_secs"] += slate.dur
                        changed = True
                while self.published and now - self.published[0].t > RETAIN_SECS:
                    if self.published.popleft().disc:
                        self.disc_seq += 1
                    changed = True
                if changed:
                    self.cond.notify_all()

    # -- serving
    def playlist(self):
        self.touch()
        with self.cond:
            self.cond.wait_for(lambda: self.published or self.stopped, FIRST_WAIT + 2)
            if not self.published:
                return None
            segs = list(self.published)
            total, first = 0.0, len(segs)
            while first > 0 and total < PLAYLIST_SECS:
                first -= 1
                total += segs[first].dur
            disc_seq = self.disc_seq + sum(1 for s in segs[:first] if s.disc)
            target = max([self.target] + [s.dur for s in segs])
        out = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{int(target + 0.999)}",
               f"#EXT-X-MEDIA-SEQUENCE:{segs[first].n}", f"#EXT-X-DISCONTINUITY-SEQUENCE:{disc_seq}"]
        for s in segs[first:]:
            if s.disc:  # also on the first entry, or the sequence count above would go wrong
                out.append("#EXT-X-DISCONTINUITY")
            out.append(f"#EXTINF:{s.dur:.3f},")
            out.append(f"/seg/{quote(self.channel['id'], safe='')}/{self.id}/{s.n}.ts")
        return "\n".join(out) + "\n"

    def segment(self, n):
        self.touch()
        with self.cond:
            for s in reversed(self.published):
                if s.n == n:
                    return s
                if s.n < n:
                    break
        return None

    def stream(self, seg, sent):
        """Blocks until more of an in-flight segment is available. Returns (bytes, finished)."""
        with self.cond:
            self.cond.wait_for(lambda: len(seg.data) > sent or seg.done or self.stopped, 10)
            self.touch()
            return bytes(seg.data[sent:]), seg.done or self.stopped

    def describe(self):
        with self.cond:
            segs = len(self.published)
        line = self.line
        return {"channel": self.channel["name"], "line": line.url if line else None,
                "rt": round(line.rt, 2) if line and line.rt else None,
                "cushion_secs": round(self.cushion(), 1),
                "published": segs, "age_secs": int(time.monotonic() - self.created),
                "switches": self.stats["switches"], "slate_secs": round(self.stats["slate_secs"], 1),
                "history": self.stats["history"][-5:]}


# ---- gateway ----------------------------------------------------------------------------------
class Gateway:
    def __init__(self, lines_path, state_dir, slate_path):
        self.lines_path = Path(lines_path)
        self.lines_mtime = 0
        self.channels, self.order, self.epg = {}, [], ""
        self.sessions = {}
        self.lock = threading.Lock()
        self.closed = threading.Event()
        state_dir = Path(state_dir) if state_dir else None
        if state_dir:
            state_dir.mkdir(parents=True, exist_ok=True)
        self.health = Health(state_dir / "health.json" if state_dir else None)
        self.slate, self.slate_dur = None, 1.2
        if slate_path and Path(slate_path).exists():
            self.slate = Path(slate_path).read_bytes()
        self.reload()
        threading.Thread(target=self._janitor, daemon=True, name="janitor").start()

    def reload(self):
        try:
            mtime = self.lines_path.stat().st_mtime
            if mtime == self.lines_mtime:
                return
            doc = json.loads(self.lines_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            log.error("cannot read %s: %s", self.lines_path, e)
            return
        decoys.clear()
        decoys.update(KNOWN_DECOYS, doc.get("decoys", []))
        self.order = [c for c in doc["channels"] if c.get("lines")]
        self.channels = {c["id"]: c for c in self.order}
        self.epg = doc.get("epg", "")
        self.lines_mtime = mtime
        log.info("loaded %d channels", len(self.order))

    def session(self, cid):
        channel = self.channels.get(cid)
        if channel is None:
            return None
        with self.lock:
            s = self.sessions.get(cid)
            if s is None or s.stopped:
                while len(self.sessions) >= MAX_SESSIONS:
                    old = min(self.sessions.values(), key=lambda x: x.last_seen)
                    old.stop()
                    del self.sessions[old.channel["id"]]
                s = self.sessions[cid] = Session(self, channel)
            return s

    def drop(self, session):
        session.stop()
        with self.lock:
            if self.sessions.get(session.channel["id"]) is session:
                del self.sessions[session.channel["id"]]

    def busy(self):
        return bool(self.sessions)

    def close(self):
        self.closed.set()
        with self.lock:
            for s in self.sessions.values():
                s.stop()
            self.sessions.clear()

    def _janitor(self):
        while not self.closed.wait(2):
            now = time.monotonic()
            with self.lock:
                for cid, s in list(self.sessions.items()):
                    if now - s.last_seen > IDLE_SECS:
                        log.info("%s: idle, stopping", cid)
                        s.stop()
                        del self.sessions[cid]
            self.reload()
            self.health.save()

    def m3u(self, host):
        head = f'#EXTM3U x-tvg-url="{self.epg}"' if self.epg else "#EXTM3U"
        out = [head]
        for c in self.order:
            logo = f' tvg-logo="{c["logo"]}"' if c.get("logo") else ""
            out.append(f'#EXTINF:-1 tvg-id="{c["id"]}" tvg-name="{c["id"]}"{logo} group-title="{c["group"]}",{c["name"]}')
            out.append(f"http://{host}/live/{quote(c['id'], safe='')}.m3u8")
        return "\n".join(out) + "\n"

    def status(self):
        h = self.health.data
        return {"channels": len(self.order), "lines": sum(len(c["lines"]) for c in self.order),
                "lines_healthy": sum(1 for v in h.values() if v["ok"] >= 0.5),
                "lines_known": len(h), "sessions": [s.describe() for s in list(self.sessions.values())]}


class Monitor(threading.Thread):
    """Checks lines slowly in the background, so tune-ins start from current knowledge. It pauses
    while anything is being watched: the viewer gets the bandwidth."""

    def __init__(self, gw, gap=MONITOR_GAP):
        super().__init__(daemon=True, name="monitor")
        self.gw, self.gap = gw, gap

    def run(self):
        while True:
            urls = [u for c in self.gw.order for u in c["lines"]]
            random.shuffle(urls)
            for url in urls:
                time.sleep(self.gap)
                while self.gw.busy():
                    time.sleep(5)
                self.check(url)

    def check(self, url):
        line = Line(url)
        try:
            pl = line.load()
            first_edge = pl.segs[-1][2]
            time.sleep(min(pl.target * 1.3, 15))
            pl = line.load()
            if pl.segs[-1][2] == first_edge:
                raise LineError("stale playlist")
            seg = pl.segs[max(0, len(pl.segs) - 3)]
            t0 = time.monotonic()
            status, data, _, _ = line.fetch.get(seg[2], 12.0, sample_bytes=MONITOR_BYTES)
            if status != 200:
                raise LineError(f"segment http {status}")
            if not looks_like_ts(data):
                raise LineError("segment is not MPEG-TS")
            rate = len(data) * 8 / max(time.monotonic() - t0, 0.01)
            self.gw.health.note(url, True, rt=rate / TYPICAL_BPS, weight=0.15)
        except LineError as e:
            self.gw.health.note(url, False, why=str(e), weight=0.15)
        except Exception as e:
            self.gw.health.note(url, False, why=type(e).__name__, weight=0.15)
        finally:
            line.fetch.close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True
    timeout = 30
    gw = None

    def log_message(self, fmt, *args):
        log.debug("%s %s", self.address_string(), fmt % args)

    def _send(self, status, body, ctype, cache="no-store"):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_segment(self, session, seg):
        if seg.done:
            return self._send(200, bytes(seg.data), "video/mp2t", "max-age=300")
        self.send_response(200)  # still arriving: pass it on as it comes
        self.send_header("Content-Type", "video/mp2t")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        sent, finished = 0, False
        while not finished:
            chunk, finished = session.stream(seg, sent)
            if chunk:
                sent += len(chunk)
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
        self.wfile.write(b"0\r\n\r\n")

    def do_GET(self):
        try:
            self._route()
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            self.close_connection = True

    def _route(self):
        path = urlsplit(self.path).path
        gw = self.gw
        if path == "/tv.m3u":
            host = self.headers.get("Host") or f"{self.server.server_address[0]}:{self.server.server_address[1]}"
            return self._send(200, gw.m3u(host).encode("utf-8"), "audio/x-mpegurl; charset=utf-8")
        if path in ("/", "/watch"):
            return self._send(200, Path(__file__).with_name("watch.html").read_bytes(), "text/html; charset=utf-8")
        if path == "/hls.min.js":
            return self._send(200, HLS_JS.read_bytes(), "text/javascript", "max-age=86400")
        if path == "/status":
            return self._send(200, json.dumps(gw.status(), ensure_ascii=False, indent=1).encode("utf-8"),
                              "application/json; charset=utf-8")
        if path.startswith("/live/") and path.endswith(".m3u8"):
            s = gw.session(unquote(path[6:-5]))
            if s is None:
                return self._send(404, b"unknown channel\n", "text/plain")
            text = s.playlist()
            if text is None:
                return self._send(503, b"no line available\n", "text/plain")
            return self._send(200, text.encode("ascii"), "application/vnd.apple.mpegurl")
        if path.startswith("/switch/"):  # for a line that plays but shows the wrong thing
            s = gw.sessions.get(unquote(path[8:]))
            if s is None:
                return self._send(404, b"that channel is not playing\n", "text/plain")
            s.leave = True
            return self._send(200, b"switching\n", "text/plain")
        if path.startswith("/seg/"):
            parts = path.split("/")
            if len(parts) == 5 and parts[4].endswith(".ts") and parts[4][:-3].isdigit():
                s = gw.sessions.get(unquote(parts[2]))
                if s is not None and s.id == parts[3]:
                    seg = s.segment(int(parts[4][:-3]))
                    if seg is not None:
                        return self._send_segment(s, seg)
            return self._send(404, b"segment gone\n", "text/plain")
        return self._send(404, b"not found\n", "text/plain")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--lines", default=str(Path(__file__).with_name("lines.json")))
    ap.add_argument("--state", default=None, help="directory for health data")
    ap.add_argument("--slate", default=str(Path(__file__).parent.parent / "assets" / "nosignal.ts"))
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8088)
    ap.add_argument("--no-monitor", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    gw = Gateway(a.lines, a.state, a.slate)
    if not a.no_monitor:
        Monitor(gw).start()
    Handler.gw = gw
    server = Server((a.bind, a.port), Handler)
    log.info("listening on %s:%d", a.bind, a.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
