#!/usr/bin/env python3
"""Fault-injection tests: real HTTP between a fake upstream, the gateway and a virtual player.

Each test breaks a line in a way public lines really do break (dead, frozen playlist, too slow, wrong
content) while the virtual player from tools/hlssim.py watches, and asserts on what a viewer would
see. Timings are scaled down with short segments, so the suite takes a couple of minutes.

    python gateway/test_gateway.py
"""
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, urlsplit

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
import build_playlist as build  # noqa: E402
import hlssim  # noqa: E402
import iptv_gateway as gw  # noqa: E402

PACKET = bytes([0x47, 0x1f, 0xff, 0x10]) + bytes(184)  # an MPEG-TS null packet


class Origin:
    """A fake live HLS server whose failures can be switched on while it runs."""

    def __init__(self, target=1.0, window=30, bytes_per_seg=120_000, media_sequence=True):
        self.target, self.window, self.size = target, window, bytes_per_seg
        self.media_sequence = media_sequence
        self.t0 = time.time() - window * target - 5  # already live, with a full window behind it
        self.dead = self.frozen = self.garbage = self.seg404 = False
        self.throttle = None  # seconds per segment
        self.requests = {"playlist": 0, "segment": 0}
        self.rolls = 0
        self.destinations = ["/live.m3u8"]
        self.user_agents = set()
        origin = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            disable_nagle_algorithm = True  # or every response waits on a delayed ACK

            def log_message(self, *a):
                pass

            def do_GET(self):
                path = urlsplit(self.path).path
                origin.user_agents.add(self.headers.get("User-Agent"))
                if origin.dead:
                    self.connection.close()  # as a dead relay does: no HTTP answer at all
                    self.close_connection = True
                    return
                if path == "/redirect.m3u8":  # a redirector choosing a destination per request
                    origin.rolls += 1
                    return self._redirect(origin.destinations[(origin.rolls - 1) % len(origin.destinations)])
                if path == "/gone.m3u8":
                    return self._reply(403, b"token expired", "text/plain")
                if path in ("/live.m3u8", "/applive/mkt.m3u8", "/other/live.m3u8"):
                    origin.requests["playlist"] += 1
                    body = origin.playlist().encode()
                    ctype = "application/vnd.apple.mpegurl"
                elif path == "/master.m3u8":
                    body = (b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=900000\nlive.m3u8\n"
                            b"#EXT-X-STREAM-INF:BANDWIDTH=90000000\nhuge.m3u8\n")
                    ctype = "application/vnd.apple.mpegurl"
                elif "/s" in path and path.endswith(".ts"):
                    origin.requests["segment"] += 1
                    if origin.seg404:
                        return self._reply(404, b"", "text/plain")
                    if origin.garbage:
                        return self._reply(200, b"<html>blocked</html>" * 5000, "text/html")
                    body = PACKET * (origin.size // 188)
                    ctype = "video/mp2t"
                    if origin.throttle:
                        self.send_response(200)
                        self.send_header("Content-Type", ctype)
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        step = len(body) // 20
                        for i in range(0, len(body), step):
                            self.wfile.write(body[i:i + step])
                            time.sleep(origin.throttle / 20)
                        return
                else:
                    return self._reply(404, b"", "text/plain")
                self._reply(200, body, ctype)

            def _redirect(self, location):
                self.send_response(302)
                self.send_header("Location", location)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def _reply(self, status, body, ctype):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/live.m3u8"
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def edge(self):
        if self.frozen:
            if isinstance(self.frozen, bool):
                self.frozen = int((time.time() - self.t0) / self.target) + 1000
            return self.frozen
        return int((time.time() - self.t0) / self.target) + 1000

    def playlist(self):
        edge = self.edge()
        first = max(1000, edge - self.window + 1)
        out = ["#EXTM3U", "#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{int(self.target + 0.999)}"]
        if self.media_sequence:
            out.append(f"#EXT-X-MEDIA-SEQUENCE:{first}")
        for n in range(first, edge + 1):
            out += [f"#EXTINF:{self.target:.3f},", f"s{n}.ts"]
        return "\n".join(out) + "\n"

    def close(self):
        self.dead = True
        self.server.shutdown()


class Harness(unittest.TestCase):
    def setUp(self):
        self.origins = []
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        for o in self.origins:
            o.close()
        if getattr(self, "server", None):
            self.server.shutdown()
            self.server.server_close()
        if getattr(self, "gateway", None):
            self.gateway.close()
        time.sleep(0.3)
        self.tmp.cleanup()

    def origin(self, **kw):
        o = Origin(**kw)
        self.origins.append(o)
        return o

    def start(self, *urls, cid="T1"):
        lines = Path(self.tmp.name) / "lines.json"
        lines.write_text(json.dumps({"channels": [
            {"id": cid, "name": cid, "group": "测试", "lines": list(urls)}]}), encoding="utf-8")
        slate = Path(self.tmp.name) / "slate.ts"
        slate.write_bytes(PACKET * 2000)
        self.gateway = gw.Gateway(lines, self.tmp.name, slate)
        self.gateway.slate_dur = 1.0
        handler = type("H", (gw.Handler,), {"gw": self.gateway})
        self.server = gw.Server(("127.0.0.1", 0), handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{self.server.server_address[1]}/live/{quote(cid, safe='')}.m3u8"

    def play(self, url, secs, results):
        results.append(hlssim.simulate(url, secs))

    def background_play(self, url, secs):
        results = []
        t = threading.Thread(target=self.play, args=(url, secs, results))
        t.start()
        return t, results

    def session(self):
        return next(iter(self.gateway.sessions.values()))

    def in_use(self, a, b):
        """Lines are raced, so either can win: returns (the one in use, the spare)."""
        url = self.session().line.url
        self.assertIn(url, (a.url, b.url))
        return (a, b) if url == a.url else (b, a)


class Failover(Harness):
    def test_healthy_line_plays_clean_and_starts_quickly(self):
        o = self.origin()
        r = hlssim.simulate(self.start(o.url), 12)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(r["stalls"], 0, r)
        self.assertLess(r["startup"], 3.0, r)
        self.assertGreater(r["played"], 8, r)

    def test_upstream_sees_the_player_user_agent(self):
        o = self.origin()
        hlssim.simulate(self.start(o.url), 3)
        self.assertEqual(o.user_agents, {"ExoPlayer"})

    def test_dead_line_is_replaced_without_a_stall(self):
        a, b = self.origin(), self.origin()
        url = self.start(a.url, b.url)
        t, res = self.background_play(url, 46)  # long enough to play through the 30 s buffer
        time.sleep(9)
        victim, spare = self.in_use(a, b)
        victim.dead = True
        time.sleep(6)
        self.assertEqual(self.session().line.url, spare.url)  # replaced within seconds
        t.join()
        r = res[0]
        self.assertIsNone(r["failed"], r)
        self.assertEqual(r["stalls"], 0, r)
        self.assertGreaterEqual(r["discontinuities"], 1, r)  # the player was told about the join

    def test_frozen_playlist_is_replaced_without_a_stall(self):
        # a frozen line answers 200 for ever, so the player alone would never leave it
        a, b = self.origin(), self.origin()
        url = self.start(a.url, b.url)
        t, res = self.background_play(url, 30)
        time.sleep(9)
        victim, spare = self.in_use(a, b)
        victim.frozen = True
        time.sleep(11)
        self.assertEqual(self.session().line.url, spare.url)
        t.join()
        self.assertEqual(res[0]["stalls"], 0, res[0])
        self.assertIsNone(res[0]["failed"], res[0])

    def test_slow_line_is_abandoned(self):
        # slow is the case the player cannot see: nothing has failed, it just keeps stalling
        a, b = self.origin(), self.origin()
        url = self.start(a.url, b.url)
        saved = gw.BUFFER_SECS
        gw.BUFFER_SECS = 12  # the gateway acts once half the buffer is gone; keep that wait short
        self.addCleanup(setattr, gw, "BUFFER_SECS", saved)
        t, res = self.background_play(url, 32)
        time.sleep(5)
        victim, spare = self.in_use(a, b)
        victim.throttle = 1.6  # 1 s segments taking 1.6 s
        time.sleep(24)
        self.assertEqual(self.session().line.url, spare.url)
        t.join()
        self.assertEqual(res[0]["stalls"], 0, res[0])

    def test_dead_first_line_costs_no_timeout_at_tune_in(self):
        a, b = self.origin(), self.origin()
        a.dead = True
        r = hlssim.simulate(self.start(a.url, b.url), 8)
        self.assertIsNone(r["failed"], r)
        self.assertLess(r["startup"], 3.0, r)  # the player's own failover would cost up to 15 s

    def test_non_video_answer_is_not_treated_as_working(self):
        a, b = self.origin(), self.origin()
        a.garbage = True
        r = hlssim.simulate(self.start(a.url, b.url), 6)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(self.session().line.url, b.url)

    def test_segment_404_moves_to_another_line(self):
        a, b = self.origin(), self.origin()
        url = self.start(a.url, b.url)
        t, res = self.background_play(url, 18)
        time.sleep(7)
        victim, spare = self.in_use(a, b)
        victim.seg404 = True
        t.join()
        self.assertEqual(res[0]["stalls"], 0, res[0])
        self.assertIsNone(res[0]["failed"], res[0])

    def test_every_line_down_shows_slate_then_recovers_alone(self):
        a = self.origin()
        url = self.start(a.url)
        t, res = self.background_play(url, 66)
        time.sleep(8)
        a.dead = True
        time.sleep(42)  # the 30 s buffer plays out first
        session = self.session()
        self.assertGreater(session.stats["slate_secs"], 0, session.describe())
        a.dead = False
        t.join()
        r = res[0]
        self.assertIsNone(r["failed"], r)  # the player never saw an error
        self.assertIsNotNone(self.session().line, "should be back on the line")

    def test_nothing_working_at_tune_in_still_answers_the_player(self):
        a = self.origin()
        a.dead = True
        url = self.start(a.url)
        t0 = time.time()
        status, body, _ = hlssim.http_get(url)
        self.assertEqual(status, 200)
        self.assertLess(time.time() - t0, 14)  # the player gives up at 15 s
        self.assertIn(b"#EXTINF", body)


class LongSegments(Harness):
    """Most public relays publish three 10 s segments, which leaves far less room than a long
    window of short segments. Segment lengths here are scaled to 4 s to keep the test short."""

    def test_no_slate_at_startup_while_the_next_segment_downloads(self):
        o = self.origin(target=4.0, window=3, bytes_per_seg=400_000)
        o.throttle = 2.0  # each 4 s segment takes 2 s, as a 10 s one takes about 3 s on a real line
        r = hlssim.simulate(self.start(o.url), 16)
        session = self.session()
        self.assertEqual(session.stats["slate_secs"], 0, session.describe())
        self.assertIsNone(r["failed"], r)
        self.assertEqual(r["stalls"], 0, r)

    def test_dead_line_inside_a_short_window(self):
        a, b = self.origin(target=4.0, window=3), self.origin(target=4.0, window=3)
        url = self.start(a.url, b.url)
        t, res = self.background_play(url, 34)
        time.sleep(12)
        victim, spare = self.in_use(a, b)
        victim.frozen = True
        t.join()
        session = self.session()
        self.assertEqual(res[0]["stalls"], 0, res[0])
        self.assertIsNone(res[0]["failed"], res[0])
        self.assertEqual(session.stats["slate_secs"], 0, session.describe())
        self.assertEqual(session.line.url, spare.url)


class Choosing(Harness):
    def test_a_line_that_just_failed_cannot_win_the_replacement_race(self):
        a, b = self.origin(), self.origin()
        url = self.start(a.url, b.url)
        hlssim.simulate(url, 3)
        session = self.session()
        session.cool[a.url] = time.monotonic() + 60
        fresh, cooling = session._candidates()
        self.assertEqual(fresh, [b.url])
        self.assertEqual(cooling, [a.url])


class ByHand(Harness):
    def test_switch_moves_to_another_line(self):
        a, b = self.origin(), self.origin()
        url = self.start(a.url, b.url)
        t, res = self.background_play(url, 10)
        time.sleep(5)
        _, spare = self.in_use(a, b)
        base = url.split("/live/")[0]
        self.assertEqual(hlssim.http_get(base + "/switch/T1")[0], 200)
        time.sleep(4)
        self.assertEqual(self.session().line.url, spare.url)
        t.join()
        self.assertIsNone(res[0]["failed"], res[0])

    def test_switch_on_a_channel_not_playing_is_404(self):
        url = self.start(self.origin().url)
        self.assertEqual(hlssim.http_get(url.split("/live/")[0] + "/switch/T1")[0], 404)


class Redirects(Harness):
    """Relay URLs choose a destination per request, sometimes an advert loop in place of the channel."""

    def redirector(self, *destinations):
        o = self.origin()
        o.destinations = list(destinations)
        return o, o.base + "/redirect.m3u8"

    def test_advert_is_refused_and_the_roll_retried(self):
        o, url = self.redirector("/applive/mkt.m3u8", "/live.m3u8")
        r = hlssim.simulate(self.start(url), 6)
        self.assertIsNone(r["failed"], r)
        self.assertTrue(self.session().line.media_url.endswith("/live.m3u8"), self.session().line.media_url)

    def test_line_that_only_ever_gives_adverts_is_not_used(self):
        o, url = self.redirector("/applive/mkt.m3u8")
        good = self.origin()
        r = hlssim.simulate(self.start(url, good.url), 6)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(self.session().line.url, good.url)

    def test_destination_is_pinned_so_a_refresh_cannot_land_on_an_advert(self):
        o, url = self.redirector("/live.m3u8", "/applive/mkt.m3u8", "/other/live.m3u8")
        playing = self.start(url)
        r = hlssim.simulate(playing, 12)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(self.session().stats["switches"], 0, self.session().describe())
        self.assertEqual(o.rolls, 1, "the redirector must only be asked once")
        self.assertEqual(r["discontinuities"], 0, r)

    def test_expired_address_goes_back_to_the_redirector(self):
        o, url = self.redirector("/live.m3u8")
        playing = self.start(url)
        t, res = self.background_play(playing, 26)
        time.sleep(6)
        line = self.session().line
        line.media_url = o.base + "/gone.m3u8"  # as when a token in the pinned address runs out
        time.sleep(6)
        self.assertEqual(self.session().line.media_url, o.base + "/live.m3u8")
        self.assertEqual(self.session().stats["switches"], 0, self.session().describe())
        t.join()
        self.assertIsNone(res[0]["failed"], res[0])
        self.assertEqual(res[0]["stalls"], 0, res[0])

    def test_extra_decoys_come_from_the_lines_file(self):
        o = self.origin()
        self.start(o.url)
        doc = json.loads(self.gateway.lines_path.read_text(encoding="utf-8"))
        doc["decoys"] = ["/live.m3u8"]
        self.gateway.lines_path.write_text(json.dumps(doc), encoding="utf-8")
        future = time.time() + 5
        os.utime(self.gateway.lines_path, (future, future))
        self.gateway.reload()
        self.addCleanup(gw.decoys.discard, "/live.m3u8")
        self.assertTrue(gw.is_decoy("http://x:1/live.m3u8?t=1"))
        self.assertFalse(gw.is_decoy("http://x:1/other.m3u8"))


class Decoys(unittest.TestCase):
    def test_a_stream_many_channels_lead_to_is_a_decoy(self):
        found = build.find_decoys({"http://1.2.3.4:88/x/ad.m3u8": {"CCTV1", "CCTV2", "湖南卫视"}})
        self.assertIn("/x/ad.m3u8", found)
        self.assertIn("1.2.3.4:88/x/ad.m3u8", found)

    def test_one_advert_spread_over_servers_is_found_by_path(self):
        found = build.find_decoys({"http://1.1.1.1/x/ad.m3u8": {"A", "B"}, "http://2.2.2.2/x/ad.m3u8": {"C"}})
        self.assertIn("/x/ad.m3u8", found)

    def test_two_names_for_one_channel_are_not_a_decoy(self):
        self.assertEqual(build.find_decoys({"http://1.1.1.1/163189/fhzw": {"凤凰中文", "凤凰卫视"}}), set())

    def test_generic_file_names_across_servers_are_not_decoys(self):
        found = build.find_decoys({f"http://{i}.1.1.1/index.m3u8": {f"C{i}"} for i in range(5)})
        self.assertEqual(found, set())


class Parsing(Harness):
    def test_playlist_without_media_sequence(self):
        o = self.origin(media_sequence=False)
        r = hlssim.simulate(self.start(o.url), 10)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(r["stalls"], 0, r)

    def test_master_playlist_picks_a_carriable_variant(self):
        o = self.origin()
        r = hlssim.simulate(self.start(o.base + "/master.m3u8"), 6)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(self.session().line.media_url, o.base + "/live.m3u8")

    def test_short_upstream_window(self):
        o = self.origin(window=3)
        r = hlssim.simulate(self.start(o.url), 12)
        self.assertIsNone(r["failed"], r)
        self.assertEqual(r["stalls"], 0, r)

    def test_published_sequence_is_contiguous(self):
        o = self.origin()
        url = self.start(o.url)
        hlssim.simulate(url, 4)
        status, body, _ = hlssim.http_get(url)
        pl = hlssim.parse_media(body.decode(), url)
        nums = [int(s[2].rsplit("/", 1)[1][:-3]) for s in pl["segs"]]
        self.assertEqual(nums, list(range(nums[0], nums[0] + len(nums))))
        self.assertEqual(pl["seq"], nums[0])

    def test_discontinuity_sequence_counts_what_left_the_window(self):
        o = self.origin()
        url = self.start(o.url)
        hlssim.simulate(url, 3)
        s = self.session()
        with s.cond:
            self.assertGreaterEqual(len(s.published), 3)
            for seg in list(s.published)[:2]:
                seg.disc = True
        saved = gw.PLAYLIST_SECS
        gw.PLAYLIST_SECS = 0.5  # only the newest segment stays listed
        try:
            _, body, _ = hlssim.http_get(url)
        finally:
            gw.PLAYLIST_SECS = saved
        self.assertIn(b"#EXT-X-DISCONTINUITY-SEQUENCE:2", body)


class Sessions(Harness):
    def test_idle_session_stops_pulling(self):
        saved = gw.IDLE_SECS
        gw.IDLE_SECS = 3
        try:
            o = self.origin()
            url = self.start(o.url)
            hlssim.simulate(url, 3)
            self.assertTrue(self.gateway.sessions)
            time.sleep(7)
            self.assertFalse(self.gateway.sessions)
            before = o.requests["segment"]
            time.sleep(3)
            self.assertEqual(o.requests["segment"], before)
        finally:
            gw.IDLE_SECS = saved

    def test_m3u_uses_the_host_the_player_asked_for(self):
        o = self.origin()
        url = self.start(o.url, cid="湖南卫视")
        base = url.split("/live/")[0]
        req = urllib.request.Request(base + "/tv.m3u", headers={"Host": "tv.home:8088"})
        text = urllib.request.urlopen(req).read().decode("utf-8")
        self.assertTrue(text.startswith("#EXTM3U"))
        self.assertIn("http://tv.home:8088/live/%E6%B9%96%E5%8D%97%E5%8D%AB%E8%A7%86.m3u8", text)
        self.assertIn('group-title="测试",湖南卫视', text)
        r = hlssim.simulate(url, 4)  # and the percent-encoded channel id routes correctly
        self.assertIsNone(r["failed"], r)

    def test_unknown_channel_is_404(self):
        url = self.start(self.origin().url)
        self.assertEqual(hlssim.http_get(url.replace("T1", "nope"))[0], 404)

    def test_health_survives_restart_and_ranks_failures_last(self):
        path = Path(self.tmp.name) / "h.json"
        h = gw.Health(path)
        h.note("good", True, rt=4.0)
        h.note("bad", False, why="x")
        h.save()
        again = gw.Health(path)
        self.assertEqual(again.rank(["bad", "unknown", "good"]), ["good", "unknown", "bad"])

    def test_lines_file_is_reloaded_while_running(self):
        o = self.origin()
        self.start(o.url)
        doc = {"channels": [{"id": "NEW", "name": "NEW", "group": "g", "lines": [o.url]}]}
        path = self.gateway.lines_path
        time.sleep(0.05)
        path.write_text(json.dumps(doc), encoding="utf-8")
        os.utime(path, (time.time() + 5, time.time() + 5))
        self.gateway.reload()
        self.assertIn("NEW", self.gateway.channels)


if __name__ == "__main__":
    unittest.main(verbosity=2)
