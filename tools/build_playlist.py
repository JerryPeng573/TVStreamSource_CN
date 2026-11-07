#!/usr/bin/env python3
"""Test public IPTV lists from this connection and build the line database for the home gateway.

Writes two files:

  gateway/lines.json   every usable line per channel, for the gateway (which ranks them live)
  au.m3u               up to three direct lines per channel, as a fallback without the gateway

What a line has to prove, and why. Each check exists because a line failed it in practice:

  * It answers to the player's own User-Agent ("ExoPlayer"), not a browser's.
  * Its playlist moves. A frozen line still answers 200 with a valid playlist, so a one-shot test
    passes it. In one test 14 of 150 lines were frozen like this.
  * It leads to the channel. Most relay URLs are redirectors that pick a destination per request,
    and many hand out TV-shopping advert loops in place of the channel: 43 of 72 channels did, for
    22% of requests. Those adverts are perfectly healthy streams, so only asking several times and
    seeing many channels land on one stream gives them away. Such streams are written to lines.json
    as decoys, and the gateway refuses them.
  * Its segments are real MPEG-TS the TV can decode (H.264 or HEVC), not an error page.
  * It delivers a whole segment faster than real time.

Backup lines only help if they fail independently. One relay operator can own many addresses (one
Kansas City operator held most of the old list, and a single server of theirs took 38 lines down
at once), so lines are spread across networks by autonomous system, not just by address.

    python tools/build_playlist.py                     # about 20 minutes
    python tools/build_playlist.py --health ~/.local/state/iptv-gateway/health.json

Standard library only.
"""
import argparse
import json
import re
import socket
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "gateway"))
from iptv_gateway import (KNOWN_DECOYS, Fetcher, Line, LineError, looks_like_ts, pick_variant,  # noqa: E402
                          segment_id, stream_keys)

EPG = "https://live.fanmingming.cn/e.xml"
LOGO = "https://live.fanmingming.cn/tv/{}.png"
BROWSER_UA = "Mozilla/5.0 (Linux; Android 9) AppleWebKit/537.36 Chrome/120 Safari/537.36"

SOURCES = {
    "guovin": "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u",
    "suxuang": "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "vbskycn": "https://live.zbds.top/tv/iptv4.m3u",
    "yuechan": "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "yang": "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "kimentanm": "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "hujingguang": "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
    "burningc4": "https://raw.githubusercontent.com/BurningC4/Chinese-IPTV/master/TV-IPV4.m3u",
    "zwc": "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.m3u",
    "freetv-cn": "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_china.m3u8",
    "iptv-org-zho": "https://iptv-org.github.io/iptv/languages/zho.m3u",
    "iptv-org-cn": "https://iptv-org.github.io/iptv/countries/cn.m3u",
    "iptv-org-hk": "https://iptv-org.github.io/iptv/countries/hk.m3u",
    "iptv-org-tw": "https://iptv-org.github.io/iptv/countries/tw.m3u",
    "best-fan-all": "https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_all.m3u8",
    "best-fan-pay": "https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_pay.m3u8",
    "fanmingming-v4": "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/index.m3u",
}

# Distinct autonomous systems that are one operator, so they do not count as independent backups.
SAME_OPERATOR = {"AS32097": "AS33387"}  # WholeSale Internet and Nocix

CCTV_TITLES = {
    "1": "综合", "2": "财经", "3": "综艺", "4": "中文国际", "5": "体育", "5+": "体育赛事",
    "6": "电影", "7": "国防军事", "8": "电视剧", "9": "纪录", "10": "科教", "11": "戏曲",
    "12": "社会与法", "13": "新闻", "14": "少儿", "15": "音乐", "16": "奥林匹克", "17": "农业农村",
}
HKTW = (
    "凤凰", "翡翠", "明珠台", "TVB", "无线", "ViuTV", "HOY", "民视", "台视", "中视", "华视", "三立",
    "东森", "年代", "非凡", "中天", "纬来", "龙华", "公视", "大爱", "寰宇", "香港", "澳门", "澳视",
    "莲花",
)
FOREIGN = re.compile("韩国|朝鲜|日本|泰国|越南|马来|印度|俄罗斯|法国|德国|英国|美国")
EXPIRING = re.compile(r"(?i)[?&](wssecret|wstime|expire|expires|e|t|token|sign|auth|authinfo|key|st)=")
PLAYABLE_VIDEO = {0x1b: "h264", 0x24: "hevc"}
OTHER_VIDEO = {0x01: "mpeg1", 0x02: "mpeg2", 0x42: "avs", 0xd2: "avs2", 0xd4: "avs3"}
# File names many unrelated streams share, so a shared path alone says nothing about these.
GENERIC_NAME = re.compile(r"index\d*|playlist|live|master|stream\d*|chunklist\w*|mono|hls|iptv|tv|\d{1,2}")


# ---------------------------------------------------------------- parsing
def parse_m3u(text):
    out, meta = [], None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            meta = line
        elif line and not line.startswith("#") and meta:
            title = meta.rsplit(",", 1)[-1].strip()
            g = re.search(r'group-title="([^"]*)"', meta)
            tn = re.search(r'tvg-name="([^"]*)"', meta)
            out.append({"title": title, "tvg": tn.group(1) if tn else "", "group": g.group(1) if g else "", "url": line})
            meta = None
    return out


def classify(item):
    """-> (group, tvg_name, title) or None if the channel is not wanted."""
    raw = (item["tvg"] or item["title"]).strip()
    if re.search(r"(?i)CCTV\s*-?\s*4K", raw):
        return "央视", "CCTV4K", "CCTV-4K 超高清"
    m = re.search(r"(?i)CCTV\s*-?\s*(\d{1,2})\s*(\+)?", raw)
    if m and "欧洲" not in raw and "美洲" not in raw and not re.match(r"(?i)\s*K", raw[m.end():]):
        n = m.group(1).lstrip("0") + ("+" if m.group(2) else "")
        if n in CCTV_TITLES:
            return "央视", f"CCTV{n}", f"CCTV-{n} {CCTV_TITLES[n]}"
    clean = re.sub(r"(?i)[\s\-_]*(HD|SD|FHD|UHD|4K)\b|高清|超清|标清|频道|\(.*?\)|\[.*?\]|（.*?）|1080p|720p", "", raw).strip()
    if FOREIGN.search(clean):
        return None
    m = re.fullmatch(r"([一-龥]{2,4})卫视", clean)
    if m and not any(h in clean for h in HKTW[:1]):  # 凤凰卫视 goes to HK/TW below
        return "卫视", clean, clean
    # prefix match only: "翡翠台" yes, "福建漳州醉美沙滩翡翠湾" no
    if clean and len(clean) <= 12 and any(clean.lower().startswith(h.lower()) for h in HKTW):
        return "港澳台", clean, clean
    if re.search(r"电影|影院|少儿|卡通|动画|纪录|体育", clean) and "付费" not in item["group"] and len(clean) <= 14:
        return "其他", clean, clean
    return None


def ts_video(data):
    """Stream types in the first PMT of a transport stream, or None if it cannot be found."""
    sync = next((i for i in range(min(len(data) - 376, 4096)) if data[i] == 0x47 and data[i + 188] == 0x47), None)
    if sync is None:
        return None
    pmt = None
    for off in range(sync, len(data) - 188, 188):
        pkt = data[off:off + 188]
        if pkt[0] != 0x47 or not pkt[1] & 0x40:
            continue
        pid = ((pkt[1] & 0x1f) << 8) | pkt[2]
        afc = (pkt[3] >> 4) & 3
        p = 4 + (1 + pkt[4] if afc in (2, 3) else 0)
        if afc == 2 or p >= 188:
            continue
        p += 1 + pkt[p]
        if p + 12 >= 188:
            continue
        if pid == 0 and pmt is None:
            pmt = ((pkt[p + 10] & 0x1f) << 8) | pkt[p + 11]
        elif pid == pmt:
            end = min(p + 3 + (((pkt[p + 1] & 0x0f) << 8) | pkt[p + 2]) - 4, 188)
            q = p + 12 + (((pkt[p + 10] & 0x0f) << 8) | pkt[p + 11])
            types = []
            while q + 5 <= end:
                types.append(pkt[q])
                q += 5 + (((pkt[q + 3] & 0x0f) << 8) | pkt[q + 4])
            return types
    return None


# ---------------------------------------------------------------- probing
PER_HOST = 3  # one redirector can front dozens of candidates, and answers a burst by refusing it
host_slots = {}
host_slots_lock = threading.Lock()


def host_slot(url):
    """Limits how many tests hit one server at once, so a busy server is not mistaken for a dead line."""
    host = urlsplit(url).hostname
    with host_slots_lock:
        return host_slots.setdefault(host, threading.BoundedSemaphore(PER_HOST))


def quick(url):
    """Stage 1: does it answer the player with a live playlist and real video?"""
    with host_slot(url):
        return first_look(url)


def first_look(url):
    line = Line(url)
    try:
        t0 = time.monotonic()
        pl = line.load(6.0)
        seg = pl.segs[max(0, len(pl.segs) - 3)]
        status, data, _, _ = line.fetch.get(seg[2], 8.0, sample_bytes=200_000)
        if status != 200 or not looks_like_ts(data):
            return None
        types = ts_video(data)
        if types and not any(t in PLAYABLE_VIDEO for t in types) and any(t in OTHER_VIDEO for t in types):
            return None  # MPEG-2 and AVS video: the Fire TV path is H.264 / HEVC
        return {"load_ms": int((time.monotonic() - t0) * 1000), "edge": pl.segs[-1][2], "target": pl.target,
                "window": round(sum(s[1] for s in pl.segs), 1), "at": time.monotonic(),
                "hevc": bool(types and 0x24 in types)}
    except (LineError, ValueError, OSError, IndexError):
        return None
    finally:
        line.fetch.close()


def moving(url, first):
    """Stage 2: has the live edge moved since stage 1? Costs a few kilobytes."""
    wait = first["at"] + max(first["target"] * 1.5, 4) - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    line = Line(url)
    try:
        for _ in range(3):
            with host_slot(url):
                edge = line.load(6.0).segs[-1][2]
            if segment_id(edge) != segment_id(first["edge"]):
                return True
            time.sleep(max(first["target"] / 2, 1))
        return False
    except (LineError, ValueError, OSError, IndexError):
        return False
    finally:
        line.fetch.close()


def rolls(url, n=6):
    """Ask several times where a line leads: redirectors choose per request. -> stream addresses."""
    fetch = Fetcher()
    finals = []
    try:
        for _ in range(n):
            try:
                with host_slot(url):
                    status, body, final, _ = fetch.get(url, 6.0, 2_000_000)
                    if status == 200 and b"#EXT-X-STREAM-INF" in body:
                        variant = pick_variant(body.decode("utf-8", "replace"), final)
                        if variant:
                            status, _, final, _ = fetch.get(variant, 6.0, 2_000_000)
                if status == 200:
                    finals.append(final)
            except LineError:
                pass
            time.sleep(0.25)
    finally:
        fetch.close()
    return finals


def find_decoys(landed):
    """Streams that many different channels lead to. landed: {stream address: {channels}}."""
    by_path = {}
    found = set()
    for final, chans in landed.items():
        path, server = stream_keys(final)
        by_path.setdefault(path, set()).update(chans)
        if len(chans) >= 3:
            found.add(server)
    for path, chans in by_path.items():
        stem = path.rsplit("/", 1)[-1].split(".")[0].lower()
        if len(chans) >= 3 and not GENERIC_NAME.fullmatch(stem):
            found.add(path)
    return found


def speed(url):
    """Stage 3: how fast does a whole segment arrive? Run a few at a time to keep the link honest."""
    line = Line(url)
    try:
        pl = line.load(6.0)
        seg = pl.segs[max(0, len(pl.segs) - 3)]
        t0 = time.monotonic()
        status, data, _, _ = line.fetch.get(seg[2], 45.0)
        elapsed = max(time.monotonic() - t0, 0.01)
        if status != 200 or len(data) < 50_000:
            return None
        return {"rt": round(seg[1] / elapsed, 2), "mbps": round(len(data) * 8 / elapsed / 1e6, 1),
                "bitrate": round(len(data) * 8 / max(seg[1], 0.1) / 1e6, 2)}
    except (LineError, ValueError, OSError, IndexError):
        return None
    finally:
        line.fetch.close()


def networks(urls):
    """Map each URL's host to its operator. Falls back to the host name if the lookup fails."""
    hosts = {urlsplit(u).hostname for u in urls}

    def resolve(h):
        try:
            return h, socket.gethostbyname(h)
        except OSError:
            return h, None

    with ThreadPoolExecutor(24) as ex:
        ips = dict(ex.map(resolve, hosts))
    info = {}
    todo = sorted({ip for ip in ips.values() if ip})
    for i in range(0, len(todo), 100):
        body = json.dumps([{"query": ip, "fields": "query,as,countryCode"} for ip in todo[i:i + 100]]).encode()
        req = urllib.request.Request("http://ip-api.com/batch", body, {"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                for row in json.load(r):
                    info[row["query"]] = row
        except (OSError, ValueError) as e:
            print(f"  ! network lookup failed: {type(e).__name__}")
            break
        time.sleep(1.5)  # the free tier allows 15 batches a minute
    out = {}
    for h, ip in ips.items():
        row = info.get(ip) or {}
        asn = (row.get("as") or "").split(" ")[0]
        out[h] = {"net": SAME_OPERATOR.get(asn, asn) or h, "cc": row.get("countryCode", "")}
    return out


# ---------------------------------------------------------------- choosing
def score(c):
    bare_ip = re.fullmatch(r"[\d.]+", urlsplit(c["url"]).hostname or "") is not None
    return (min(c["rt"], 4.0) * 10 - c["load_ms"] / 200 + min(c["window"], 60) / 6
            + (0 if bare_ip else 4) - (8 if EXPIRING.search(c["url"]) else 0) - (3 if c["hevc"] else 0))


def spread(cands, limit, per_host=2):
    """Best lines first, but take one from every network before a second from any of them."""
    by_net = {}
    for c in sorted(cands, key=score, reverse=True):
        by_net.setdefault(c["net"], []).append(c)
    picked, hosts = [], {}
    rank = 0
    while len(picked) < limit and any(len(v) > rank for v in by_net.values()):
        for net in sorted(by_net, key=lambda n: score(by_net[n][0]), reverse=True):
            if rank < len(by_net[net]) and len(picked) < limit:
                c = by_net[net][rank]
                host = urlsplit(c["url"]).hostname
                if hosts.get(host, 0) < per_host:
                    hosts[host] = hosts.get(host, 0) + 1
                    picked.append(c)
        rank += 1
    return picked


def channel_key(k):
    order = {"央视": 0, "卫视": 1, "港澳台": 2, "其他": 3}
    grp, tvg, _ = k
    m = re.fullmatch(r"CCTV(\d+)(\+?)", tvg)
    return order[grp], int(m.group(1)) if m else 99, 1 if m and m.group(2) else 0, tvg


# ---------------------------------------------------------------- main
def load_previous(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"channels": []}


def collect(previous, direct_path, health):
    items, seen = [], set()

    def add(url, cls, src):
        h = health.get(url)
        if url in seen or (h and h.get("fails", 0) >= 6 and h.get("ok", 1) < 0.15):
            return False  # has been failing for days, whatever the lists say
        seen.add(url)
        items.append({"url": url, "cls": cls, "src": src})
        return True

    for c in previous["channels"]:  # good lines stay even when the public lists drop them
        for url in c["lines"]:
            add(url, (c["group"], c["id"], c["name"]), "previous")
    try:  # the playlist itself, so a first run keeps what is already known to work
        for it in parse_m3u(Path(direct_path).read_text(encoding="utf-8")):
            c = classify(it)
            if c:
                add(it["url"], c, "previous")
    except OSError:
        pass
    for name, url in SOURCES.items():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
            with urllib.request.urlopen(req, timeout=40) as r:
                text = r.read().decode("utf-8", "replace")
        except OSError as e:
            print(f"  ! {name}: {type(e).__name__}")
            continue
        got = 0
        for it in parse_m3u(text):
            if it["url"].startswith("http"):
                c = classify(it)
                if c and add(it["url"], c, name):
                    got += 1
        print(f"  {name}: {got} new candidates")
    return items


def pmap(fn, args, workers):
    with ThreadPoolExecutor(workers) as ex:
        return list(ex.map(lambda a: fn(*a) if isinstance(a, tuple) else fn(a), args))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(ROOT / "au.m3u"), help="direct-line fallback playlist")
    ap.add_argument("--lines", default=str(ROOT / "gateway" / "lines.json"), help="gateway line database")
    ap.add_argument("--health", default=None, help="the gateway's health.json, if it has been running")
    ap.add_argument("--max-lines", type=int, default=8, help="lines per channel for the gateway")
    ap.add_argument("--max-uris", type=int, default=3, help="lines per channel in the direct playlist")
    ap.add_argument("--min-realtime", type=float, default=1.5, help="seconds of video per second of download")
    ap.add_argument("--per-channel", type=int, default=12, help="candidates per channel given a speed test")
    ap.add_argument("--report", default=None, help="write a JSON report here")
    a = ap.parse_args()

    previous = load_previous(a.lines)
    health = load_previous(a.health) if a.health else {}
    health = {} if "channels" in health else health
    t0 = time.time()
    print(f"collecting ({len(SOURCES)} lists)...", flush=True)
    items = collect(previous, a.out, health)
    urls = [i["url"] for i in items]
    print(f"{len(items)} candidates; stage 1, answers with real video...", flush=True)
    first = dict(zip(urls, pmap(quick, urls, 48)))
    alive = [u for u in urls if first[u]]
    print(f"  {len(alive)} alive; stage 2, playlist moving and leading where it says...", flush=True)
    moved = dict(zip(alive, pmap(moving, [(u, first[u]) for u in alive], 48)))
    live = [i for i in items if moved.get(i["url"])]
    print(f"  {len(live)} moving ({len(alive) - len(live)} frozen)", flush=True)
    landed_at = dict(zip([i["url"] for i in live], pmap(rolls, [i["url"] for i in live], 24)))
    landed = {}
    for it in live:
        for final in landed_at[it["url"]]:
            landed.setdefault(final.split("?", 1)[0], set()).add(it["cls"][1])
    decoys = KNOWN_DECOYS | find_decoys(landed)
    new_decoys = sorted(decoys - KNOWN_DECOYS)
    if new_decoys:
        print(f"  detected {len(new_decoys)} more streams shared across channels: {new_decoys[:8]}", flush=True)

    def honest(url):
        return not any(k in decoys for f in landed_at[url] for k in stream_keys(f))

    clean = [i for i in live if landed_at[i["url"]] and honest(i["url"])]
    print(f"  {len(clean)} lead to their channel ({len(live) - len(clean)} led to adverts or nowhere); "
          "stage 3, speed...", flush=True)
    live = clean

    by_chan = {}
    for it in live:
        by_chan.setdefault(it["cls"], []).append(it)
    chosen = []
    for chan, its in by_chan.items():
        hosts = {}
        for it in sorted(its, key=lambda i: first[i["url"]]["load_ms"]):
            host = urlsplit(it["url"]).hostname
            if hosts.get(host, 0) < 3:  # a fourth line on one host adds nothing
                hosts[host] = hosts.get(host, 0) + 1
                chosen.append(it)
            if sum(hosts.values()) >= a.per_channel:
                break
    print(f"  {len(chosen)} to speed test", flush=True)
    speeds = dict(zip([i["url"] for i in chosen], pmap(speed, [i["url"] for i in chosen], 6)))

    def too_slow(url):
        return not speeds[url] or speeds[url]["rt"] < a.min_realtime

    # Six downloads at once share the home link, so a miss may be the test and not the line.
    again = [i["url"] for i in chosen if too_slow(i["url"])]
    print(f"  {len(again)} missed the speed bar; testing those again, two at a time...", flush=True)
    for url, result in zip(again, pmap(speed, again, 2)):
        if result and (not speeds[url] or result["rt"] > speeds[url]["rt"]):
            speeds[url] = result
    nets = networks([i["url"] for i in chosen])

    chans = {}
    for it in chosen:
        s = speeds[it["url"]]
        if not s or s["rt"] < a.min_realtime:
            continue
        f, n = first[it["url"]], nets[urlsplit(it["url"]).hostname]
        chans.setdefault(it["cls"], []).append({
            "url": it["url"], "src": it["src"], "load_ms": f["load_ms"], "window": f["window"],
            "target": f["target"], "hevc": f["hevc"],
            "wanders": len({x.split("?", 1)[0] for x in landed_at[it["url"]]}) > 1, **s, **n})

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    channels, report, direct = [], [], [f'#EXTM3U x-tvg-url="{EPG}"']
    prev = {c["id"]: c for c in previous["channels"]}
    keys = sorted(set(chans) | {(c["group"], c["id"], c["name"]) for c in previous["channels"]}, key=channel_key)
    independent = kept = 0
    for k in keys:
        grp, tvg, title = k
        entry = {"id": tvg, "name": title, "group": grp, "logo": LOGO.format(tvg)}
        if k in chans:
            lines = spread(chans[k], a.max_lines)
            independent += len({c["net"] for c in lines}) >= 2
            entry["lines"] = [c["url"] for c in lines]
            entry["networks"] = len({c["net"] for c in lines})
            # A player rereads the URL at every refresh, so a line that wanders between servers only
            # works through the gateway, which pins it.
            for c in [c for c in lines if not c["wanders"]][:a.max_uris]:
                direct.append(f'#EXTINF:-1 tvg-id="{tvg}" tvg-name="{tvg}" tvg-logo="{LOGO.format(tvg)}" group-title="{grp}",{title}')
                direct.append(c["url"])
            report.append({"channel": title, "group": grp, "lines": lines})
        else:
            # Nothing passed today. Keep the channel for a while: the gateway keeps trying its lines.
            funnel = [sum(1 for i in rows if i["cls"] == k) for rows in (items, live, chosen)]
            fast = sum(1 for i in chosen if i["cls"] == k and not too_slow(i["url"]))
            print(f"  no line for {tvg}: {funnel[0]} candidates, {funnel[1]} leading to the channel, "
                  f"{funnel[2]} speed tested, {fast} fast enough", flush=True)
            old = prev[tvg]
            since = old.get("failing_since", today)
            age = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(since, "%Y-%m-%d")).days
            if age > 14:
                continue
            entry.update(lines=old["lines"], failing_since=since)
            if "networks" in old:
                entry["networks"] = old["networks"]
            kept += 1
        channels.append(entry)

    doc = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"), "epg": EPG,
           "decoys": sorted(decoys), "channels": channels}
    tmp = Path(a.lines).with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8", newline="\n")
    tmp.replace(a.lines)
    Path(a.out).write_text("\n".join(direct) + "\n", encoding="utf-8", newline="\n")
    print(f"{len(chans)} channels with working lines, {independent} of them on 2+ independent networks; "
          f"{kept} kept from before; {(time.time() - t0) / 60:.0f} min")
    print(f"wrote {a.lines}\nwrote {a.out}")
    if a.report:
        Path(a.report).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    if a.health:  # the dashboard reads each night's result from beside the gateway's health data
        summary = {"t": int(time.time()), "minutes": round((time.time() - t0) / 60, 1), "candidates": len(items),
                   "alive": len(alive), "moving": sum(1 for u in alive if moved.get(u)),
                   "honest": len(live), "speed_tested": len(chosen), "channels_ok": len(chans),
                   "channels": len(channels), "multi_network": independent, "kept": kept,
                   "lines": sum(len(c["lines"]) for c in channels), "new_decoys": len(new_decoys),
                   "failing": [c["id"] for c in channels if c.get("failing_since")]}
        try:
            with open(Path(a.health).with_name("refresh.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps(summary, ensure_ascii=False) + "\n")
        except OSError as e:
            print(f"  ! cannot record this run: {e}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
