#!/usr/bin/env python3
"""Build a reliable m3u for my-tv-0 by testing public IPTV lists from *this* machine.

Why: a static list full of dead / geo-locked / expired-token URLs makes my-tv-0 retry
each dead URL ~10 times (across several player types) before trying the next one, so
channels load slowly or never. This script keeps only streams that actually deliver
video from where you run it, ranks them by real load time, and caps each channel at a
few URLs.

Usage (run from anywhere, ideally on a PC on the same internet connection as the TV):
    python tools/build_playlist.py                 # writes au.m3u next to the repo root
    python tools/build_playlist.py --out au.m3u --max-uris 3 --min-realtime 2.0
Requires: Python 3.9+ and `pip install requests`.
"""
import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin

import requests

EPG = "https://live.fanmingming.cn/e.xml"
LOGO = "https://live.fanmingming.cn/tv/{}.png"

SOURCES = {
    "guovin": "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u",
    "vbskycn": "https://live.zbds.top/tv/iptv4.m3u",
    "yuechan": "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "kimentanm": "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "hujingguang": "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8",
    "iptv-org-cn": "https://iptv-org.github.io/iptv/countries/cn.m3u",
    "iptv-org-hk": "https://iptv-org.github.io/iptv/countries/hk.m3u",
    "iptv-org-tw": "https://iptv-org.github.io/iptv/countries/tw.m3u",
    "best-fan-all": "https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_all.m3u8",
    "best-fan-pay": "https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_pay.m3u8",
    "fanmingming-v4": "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/index.m3u",
    "fanmingming-v6": "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u",
}

UA = "Mozilla/5.0 (Linux; Android 9) AppleWebKit/537.36 Chrome/120 Safari/537.36"

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
# query keys that usually mean the URL expires or is bound to a client
EXPIRING = re.compile(r"(?i)[?&](wssecret|wstime|expire|expires|e|t|token|sign|auth|authinfo|key|st)=")


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
    m = re.fullmatch(r"([一-龥]{2,4})卫视", clean)
    if m and not any(h in clean for h in HKTW[:1]):  # 凤凰卫视 goes to HK/TW below
        return "卫视", clean, clean
    # prefix match only: "翡翠台" yes, "福建漳州醉美沙滩翡翠湾" no
    if clean and len(clean) <= 12 and any(clean.lower().startswith(h.lower()) for h in HKTW):
        return "港澳台", clean, clean
    gl = item["group"]
    if re.search(r"电影|影院|少儿|卡通|动画|纪录|体育", clean) and "付费" not in gl and len(clean) <= 14:
        return "其他", clean, clean
    return None


# ---------------------------------------------------------------- probing
def fetch_playlist(s, url, timeout, max_bytes=512_000, max_secs=8):
    """GET a playlist but never read more than max_bytes / max_secs: some URLs are endless raw streams."""
    t0 = time.time()
    with s.get(url, timeout=timeout, stream=True, headers={"User-Agent": UA}) as resp:
        if resp.status_code != 200:
            return None, None
        buf = b""
        for chunk in resp.iter_content(16384):
            buf += chunk
            if len(buf) > max_bytes or time.time() - t0 > max_secs:
                break
        if not buf.lstrip().startswith(b"#EXTM3U"):
            return None, None
        return buf.decode("utf-8", "replace"), resp.url


def probe(url, seg_bytes, timeout=(4, 7), session=None):
    s = session or requests
    r = {"ok": False}
    t0 = time.time()
    try:
        text, base = fetch_playlist(s, url, timeout)
        if text is None:
            return r
        for _ in range(2):
            if "#EXT-X-STREAM-INF" in text:
                var = [urljoin(base, l) for l in text.splitlines() if l.strip() and not l.startswith("#")]
                if not var:
                    return r
                text, base = fetch_playlist(s, var[0], timeout)
                if text is None:
                    return r
        if "#EXT-X-ENDLIST" in text:
            return r  # VOD, not a live channel
        segs, dur = [], 0.0
        for l in text.splitlines():
            l = l.strip()
            if l.startswith("#EXTINF:"):
                try:
                    dur = float(l[8:].split(",")[0])
                except ValueError:
                    dur = 0.0
            elif l and not l.startswith("#"):
                segs.append((urljoin(base, l), dur))
        if not segs:
            return r
        playlist_ms = (time.time() - t0) * 1000
        seg, seg_dur = segs[-2] if len(segs) > 1 else segs[-1]
        t1 = time.time()
        got, complete = 0, True
        with s.get(seg, timeout=timeout, stream=True, headers={"User-Agent": UA}) as sr:
            if sr.status_code != 200:
                return r
            first_ms = (time.time() - t1) * 1000
            for chunk in sr.iter_content(65536):
                got += len(chunk)
                if got >= seg_bytes or time.time() - t1 > 10:
                    complete = False
                    break
        dt = max(time.time() - t1, 0.001)
        if got < 50_000:
            return r
        # realtime factor: seconds of video downloaded per second of waiting (>1 plays without stalling)
        rt = round(seg_dur / dt, 2) if complete and seg_dur else 0.0
        r.update(ok=True, kbps=int(got * 8 / dt / 1000), load_ms=int(playlist_ms + first_ms), rt=rt)
    except Exception:
        pass
    return r


def run_probe(urls, workers, seg_bytes):
    with requests.Session() as s:
        s.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=workers))
        s.mount("https://", requests.adapters.HTTPAdapter(pool_maxsize=workers))
        with ThreadPoolExecutor(workers) as ex:
            return dict(zip(urls, ex.map(lambda u: probe(u, seg_bytes, session=s), urls)))


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "au.m3u"))
    ap.add_argument("--max-uris", type=int, default=3, help="URLs kept per channel (each dead one costs retries)")
    ap.add_argument("--min-realtime", type=float, default=2.0,
                    help="min seconds of video downloaded per second waiting (2 = twice as fast as playback)")
    ap.add_argument("--max-load-ms", type=int, default=4000)
    ap.add_argument("--report", default=None, help="write a JSON report here")
    a = ap.parse_args()

    items, seen = [], set()
    for name, url in SOURCES.items():
        try:
            txt = requests.get(url, timeout=40, headers={"User-Agent": UA}).text
        except Exception as e:
            print(f"! {name}: {type(e).__name__}")
            continue
        got = 0
        for it in parse_m3u(txt):
            if not it["url"].startswith("http") or it["url"] in seen:
                continue
            c = classify(it)
            if not c:
                continue
            seen.add(it["url"])
            it["cls"], it["src"] = c, name
            items.append(it)
            got += 1
        print(f"  {name}: {got} candidate streams")
    print(f"{len(items)} candidates; round 1 (liveness, 48 parallel)...")
    urls = [i["url"] for i in items]
    r1 = run_probe(urls, 48, 200_000)
    alive = [u for u in urls if r1[u]["ok"]]
    print(f"  {len(alive)} alive; round 2 (speed, 6 parallel)...")
    r2 = run_probe(alive, 6, 8_000_000)  # whole segment, so the realtime factor is meaningful
    res = {u: r2[u] for u in alive if r2[u]["ok"]}
    print(f"  {len(res)} passed both rounds")

    chans = {}
    for it in items:
        r = res.get(it["url"])
        if not r or r["rt"] < a.min_realtime or r["load_ms"] > a.max_load_ms:
            continue
        grp, tvg, title = it["cls"]
        chans.setdefault((grp, tvg, title), []).append((bool(EXPIRING.search(it["url"])), r["load_ms"], r["rt"], it))

    order = {"央视": 0, "卫视": 1, "港澳台": 2, "其他": 3}

    def keyfn(k):
        grp, tvg, _ = k
        m = re.fullmatch(r"CCTV(\d+)(\+?)", tvg)
        return (order[grp], int(m.group(1)) if m else 99, 1 if m and m.group(2) else 0, tvg)

    lines = [f'#EXTM3U x-tvg-url="{EPG}"']
    report, n_uris = [], 0
    for k in sorted(chans, key=keyfn):
        grp, tvg, title = k
        cand = sorted(chans[k], key=lambda x: (x[0], x[1]))  # stable-looking URLs first, then fastest to load
        stable = [c for c in cand if not c[0]]
        pick = (stable or cand)[: a.max_uris]  # expiring-token URLs only as a last resort
        for _, load_ms, rt, it in pick:
            lines.append(f'#EXTINF:-1 tvg-id="{tvg}" tvg-name="{tvg}" tvg-logo="{LOGO.format(tvg)}" group-title="{grp}",{title}')
            lines.append(it["url"])
            n_uris += 1
            report.append({"title": title, "group": grp, "url": it["url"], "load_ms": load_ms, "realtime": rt, "src": it["src"]})
    Path(a.out).write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {a.out}: {len(chans)} channels, {n_uris} URLs")
    if a.report:
        Path(a.report).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
