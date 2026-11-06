#!/usr/bin/env python3
"""Compare what a viewer gets with and without the gateway, on real lines.

Two questions, the same ones a viewer asks:

  tune    After changing channel, how long until there is a picture, and does it ever come?
  soak    Once it is playing, how often does it stall?

The direct side follows mytv-android's own logic (MainContentState.kt): try lines in order, move on
only when a line fails, wait up to the 15 s load timeout for each, and stop after the last. Its
hosts-that-worked memory is not modelled, so the direct side gets every line tried fresh. Two things
flatter the direct side: a refused connection costs nothing here, where ExoPlayer spends several
seconds on retries, and a frozen line counts as a picture.

    python tools/compare.py tune --playlist au.m3u --gateway http://192.168.1.120:8088
    python tools/compare.py soak --playlist au.m3u --gateway http://192.168.1.120:8088 --secs 600
"""
import argparse
import json
import random
import statistics
import sys
import threading
import time
from pathlib import Path
from urllib.parse import quote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gateway"))
import hlssim  # noqa: E402
from iptv_gateway import KNOWN_DECOYS  # noqa: E402

GAVE_UP = 60.0  # what a viewer has long since stopped waiting for


def read_m3u(path):
    chans, order, meta = {}, [], None
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.startswith("#EXTINF"):
            tvg = line.split('tvg-name="', 1)[1].split('"', 1)[0]
            meta = (tvg, line.rsplit(",", 1)[1].strip())
        elif line and not line.startswith("#") and meta:
            if meta[0] not in chans:
                chans[meta[0]] = {"name": meta[1], "lines": []}
                order.append(meta[0])
            chans[meta[0]]["lines"].append(line.strip())
    return chans, order


def direct_tune(lines):
    """(seconds until a picture, shows an advert) following the player's line logic. The seconds are
    None if a picture never comes. An advert plays fine, so the player stays on it."""
    t0 = time.time()
    for url in lines:
        try:
            status, _, final = hlssim.http_get(url, timeout=8)
        except Exception:
            status, final = 0, url
        advert = status == 200 and urlsplit(final).path in KNOWN_DECOYS
        tried = time.time() - t0
        r = hlssim.simulate(url, secs=hlssim.LOAD_TIMEOUT)
        if r["startup"] is not None:
            return tried + r["startup"], advert
    return None, False


def gateway_tune(gateway, tvg):
    r = hlssim.simulate(f"{gateway}/live/{quote(tvg, safe='')}.m3u8", secs=hlssim.LOAD_TIMEOUT)
    return r["startup"], r


def summary(values):
    ok = sorted(v for v in values if v is not None)
    if not ok:
        return "nothing played"
    q = lambda p: ok[min(len(ok) - 1, int(len(ok) * p))]
    return (f"played {len(ok)}/{len(values)}, median {statistics.median(ok):.1f}s, "
            f"p90 {q(.9):.1f}s, worst {ok[-1]:.1f}s")


def tune(a):
    chans, order = read_m3u(a.playlist)
    picked = random.sample(order, min(a.channels, len(order)))
    rows = []
    for i, tvg in enumerate(picked, 1):
        direct, advert = direct_tune(chans[tvg]["lines"]) if not a.gateway_only else (None, False)
        time.sleep(1)
        gw, _ = gateway_tune(a.gateway, tvg) if a.gateway else (None, None)
        rows.append({"channel": tvg, "direct": direct, "advert": advert, "gateway": gw})
        fmt = lambda v: "never" if v is None else f"{v:5.1f}s"
        print(f"{i:3}/{len(picked)} {tvg:14} direct {fmt(direct)}{' ADVERT' if advert else '':7}  "
              f"gateway {fmt(gw)}", flush=True)
    print("\ndirect :", summary([r["direct"] for r in rows]),
          f"; {sum(r['advert'] for r in rows)} showed an advert instead of the channel")
    print("gateway:", summary([r["gateway"] for r in rows]))
    if a.out:
        Path(a.out).write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")


def soak(a):
    chans, order = read_m3u(a.playlist)
    picked = [c for c in (a.only.split(",") if a.only else random.sample(order, a.channels)) if c in chans]
    jobs = []
    for tvg in picked:
        jobs.append(("direct", tvg, chans[tvg]["lines"][0]))
        jobs.append(("gateway", tvg, f"{a.gateway}/live/{quote(tvg, safe='')}.m3u8"))
    results = []

    def run(kind, tvg, url):
        r = hlssim.simulate(url, a.secs)
        r.update(kind=kind, channel=tvg)
        results.append(r)

    threads = [threading.Thread(target=run, args=j) for j in jobs]
    for t in threads:
        t.start()
        time.sleep(1)
    for t in threads:
        t.join()
    for kind in ("direct", "gateway"):
        rows = [r for r in results if r["kind"] == kind]
        for r in rows:
            print(f'{kind:8}{r["channel"]:14} played {r["played"]:6.0f}s  stalls {r["stalls"]:3}  '
                  f'stalled {r["stall_secs"]:5.1f}s  skips {r["discontinuities"]:2}  failed {r["failed"]}')
        print(f'{kind}: {sum(r["stalls"] for r in rows)} stalls, {sum(r["stall_secs"] for r in rows):.0f}s stalled, '
              f'{sum(1 for r in rows if r["failed"])} failed, of {sum(r["played"] for r in rows):.0f}s played\n')
    if a.out:
        Path(a.out).write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("mode", choices=["tune", "soak"])
    ap.add_argument("--playlist", default=str(Path(__file__).resolve().parent.parent / "au.m3u"))
    ap.add_argument("--gateway", default=None)
    ap.add_argument("--gateway-only", action="store_true")
    ap.add_argument("--channels", type=int, default=20)
    ap.add_argument("--only", default=None, help="comma-separated channel ids")
    ap.add_argument("--secs", type=int, default=600)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    random.seed(a.seed)
    (tune if a.mode == "tune" else soak)(a)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
