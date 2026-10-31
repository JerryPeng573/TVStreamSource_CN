# TVStreamSource_CN — Australia-tested playlist for my-tv-0

`au.m3u` is a short playlist of Chinese TV channels (央视 / 卫视 / 港澳台) in which **every URL was tested
from an Australian connection** (HLS playlist fetched, a real video segment downloaded, speed and load
time measured, tested twice). Dead, geo-blocked, VOD and slow URLs are dropped, and each channel keeps at
most 3 URLs, fastest first, so my-tv-0 falls over to a working one quickly.

Current build (tested from Brisbane): 72 channels / 150 URLs; median time to first video data about
1.1 s, 90% under 2.4 s, every URL downloaded at least 2x faster than it plays when built. A second,
independent re-test of the finished file a few minutes later found 148 of 150 still alive and 144 still
at 1.5x or better. Many of the URLs are community relay servers on bare IP addresses, which can vanish
without notice, so refresh regularly (see below).

## Use in my-tv-0

Settings → video source → set the address to your raw file URL:

    https://raw.githubusercontent.com/JerryPeng573/TVStreamSource_CN/main/au.m3u

If raw.githubusercontent.com is slow on your network, the jsDelivr mirror is an alternative
(it caches, so updates can lag for a while):

    https://cdn.jsdelivr.net/gh/JerryPeng573/TVStreamSource_CN@main/au.m3u

Also switch on **应用启动后更新视频源** so the app re-downloads the list when it starts.

## Keep it fresh (important)

IPTV URLs rot within days to weeks. Re-run the tester every week or two, ideally on a PC on the
same internet connection as the TV:

    pip install requests
    python tools/build_playlist.py

It downloads ~10 public lists, tests every stream from your location, and rewrites `au.m3u`.
Then commit and push `au.m3u`.

Useful options: `--max-uris 3` (URLs per channel), `--min-realtime 2.0` (reject streams that download less than 2x faster than they play),
`--max-load-ms 4000` (reject slow-to-start streams), `--report report.json` (per-URL timings).

## Why the old list was slow and unreliable

* `tv/m3u/index.m3u` (from fanmingming/live) is mostly Guangdong Unicom IPTV URLs with an expired
  account token (`Authinfo=...&mac=...`) and China-only CDNs: from Australia ~2 of 94 worked.
* `tv/m3u/ipv6.m3u` needs IPv6 routes into China Mobile's network: 12 of 82 worked.
* my-tv-0 retries each URL up to 10 times (cycling player types) before trying the next URL for the
  same channel, so a dead first URL costs a long wait. Keeping only live URLs removes that wait.
