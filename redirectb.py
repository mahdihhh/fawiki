#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Toolforge database report bot
# Report page: ویکی‌پدیا:گزارش دیتابیس/تغییرمسیرهای از کار افتاده به بخش‌ها
#
# Author: Mahdiz
# Bot: Mahdibot
# Repository: https://github.com/mahdihhh
# License: MIT

import os
import time
import datetime as dt
from collections import defaultdict
from urllib.parse import unquote
import re
import requests
import pymysql
import pywikibot

API = "https://fa.wikipedia.org/w/api.php"

TARGET_PAGE = "ویکی‌پدیا:گزارش دیتابیس/تغییرمسیرهای از کار افتاده به بخش‌ها"
SIGN_PAGE = "ویکی‌پدیا:گزارش دیتابیس/تغییرمسیرهای از کار افتاده به بخش‌ها/امضا"

MAX_ROWS = 500
REQUEST_SLEEP = 0.12
BACKLINKS_MAX_PAGES = 20000
BACKLINKS_NAMESPACE = 0

DB_NAME = "fawiki_p"
DB_HOST = "fawiki.analytics.db.svc.wikimedia.cloud"
DB_USER = os.environ.get("TOOL_REPLICA_USER")
DB_PASS = os.environ.get("TOOL_REPLICA_PASSWORD")


def to_persian_digits(s):
    return str(s).translate(str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹"))


def db_connect():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def decode_if_bytes(v):
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v


def normalize_fragment(frag):
    try:
        frag = unquote(frag)
    except Exception:
        pass
    return (frag or "").strip()


def normalize_anchor_key(s: str) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        try:
            s = s.decode("utf-8", errors="ignore")
        except Exception:
            s = str(s)
    try:
        s = unquote(s)
    except Exception:
        pass
    s = s.strip()
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s)
    return s


def mw_unescape_id(s: str) -> str:
    if not s:
        return ""
    b = bytearray()
    i = 0
    while i < len(s):
        if s[i] == "." and i + 2 < len(s):
            hx = s[i + 1 : i + 3]
            if re.fullmatch(r"[0-9A-Fa-f]{2}", hx):
                b.append(int(hx, 16))
                i += 3
                continue
        b.extend(s[i].encode("utf-8", errors="ignore"))
        i += 1
    try:
        return b.decode("utf-8")
    except Exception:
        return s


def api_get_sections(title, session):
    params = {
        "action": "parse",
        "page": title,
        "prop": "sections",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    r = session.get(API, params=params, timeout=30)
    data = r.json()
    sections = (data.get("parse") or {}).get("sections") or []
    return {s["anchor"] for s in sections if s.get("anchor")}


def fetch_html(title, session) -> str:
    params = {
        "action": "parse",
        "page": title,
        "prop": "text",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    r = session.get(API, params=params, timeout=30)
    data = r.json()
    return ((data.get("parse") or {}).get("text")) or ""


def fetch_wikitext(title, session) -> str:
    params = {
        "action": "query",
        "titles": title,
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    r = session.get(API, params=params, timeout=30)
    data = r.json()
    pages = (data.get("query") or {}).get("pages") or []
    if not pages:
        return ""
    revs = pages[0].get("revisions") or []
    if not revs:
        return ""
    return ((revs[0].get("slots") or {}).get("main") or {}).get("content") or ""


def fragment_matches(frag, anchors):
    return (
        frag in anchors
        or frag.replace(" ", "_") in anchors
        or frag.replace("_", " ") in anchors
    )


def build_id_index_from_html(html: str) -> set[str]:
    if not html:
        return set()
    out = set()
    for m in re.finditer(r'\b(?:id|name)\s*=\s*(?:"([^"]+)"|\'([^\']+)\')', html):
        raw = m.group(1) or m.group(2) or ""
        if not raw:
            continue
        out.add(normalize_anchor_key(raw))
        out.add(normalize_anchor_key(mw_unescape_id(raw)))
    return out


def fragment_exists_in_html_index(frag: str, id_index: set[str]) -> bool:
    if not id_index:
        return False
    base = {
        frag,
        frag.replace("_", " "),
        frag.replace(" ", "_"),
    }
    wanted = set()
    for x in base:
        x = (x or "").strip()
        if not x:
            continue
        wanted.add(normalize_anchor_key(x))
    return bool(wanted & id_index)


def fragment_exists_in_wikitext(wtxt: str, frag: str) -> bool:
    if not wtxt:
        return False

    cand = {
        frag,
        frag.replace("_", " "),
        frag.replace(" ", "_"),
    }

    for f in cand:
        f = (f or "").strip()
        if not f:
            continue

        pat1 = rf"\|\s*نام\s*=\s*{re.escape(f)}\s*(?:\n|\r|}}|\|)"
        if re.search(pat1, wtxt):
            return True

        pat2 = rf"\|\s*(?:عنوان|title)\s*=\s*{re.escape(f)}\s*(?:\n|\r|}}|\|)"
        if re.search(pat2, wtxt):
            return True

    return False


def api_count_backlinks(title, session, namespace=0, max_pages=5000):
    total = 0
    cont = {}
    while True:
        params = {
            "action": "query",
            "list": "backlinks",
            "bltitle": title,
            "blnamespace": str(namespace),
            "blfilterredir": "nonredirects",
            "bllimit": "max",
            "format": "json",
            "formatversion": "2",
        }
        params.update(cont)
        r = session.get(API, params=params, timeout=30)
        data = r.json()
        bl = (data.get("query") or {}).get("backlinks") or []
        total += len(bl)
        if total >= max_pages:
            return total
        cont = (data.get("continue") or {})
        if not cont:
            return total
        time.sleep(REQUEST_SLEEP)


def fetch_candidates():
    sql = """
    SELECT
      p.page_title  AS redirect_title,
      t.page_title  AS target_title,
      r.rd_fragment AS fragment
    FROM redirect r
    JOIN page p
      ON p.page_id = r.rd_from
    JOIN page t
      ON t.page_namespace = r.rd_namespace
     AND t.page_title = r.rd_title
    WHERE r.rd_namespace = 0
      AND r.rd_fragment IS NOT NULL
      AND r.rd_fragment <> ''
      AND p.page_namespace = 0
      AND p.page_is_redirect = 1
    LIMIT %s
    """

    conn = db_connect()
    with conn.cursor() as cur:
        cur.execute(sql, (MAX_ROWS * 10,))
        rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append(
            {
                "redirect": decode_if_bytes(r["redirect_title"]).replace("_", " "),
                "target": decode_if_bytes(r["target_title"]).replace("_", " "),
                "fragment": r["fragment"],
                "incoming": 0,
            }
        )
    return out


def build_table(rows):
    lines = []
    lines.append('{| class="wikitable sortable"')
    lines.append("! ردیف !! تغییرمسیر !! پیوندهای ورودی !! هدف")
    for i, r in enumerate(rows, 1):
        lines.append("|-")
        lines.append(
            f"| {to_persian_digits(i)} || [[{r['redirect']}]] || "
            f"{to_persian_digits(r['incoming'])} || "
            f"[[{r['target']}#{r['fragment_display']}|{r['target']}#{r['fragment_display']}]]"
        )
    lines.append("|}")
    return "\n".join(lines)


def main():
    now = dt.datetime.now(dt.UTC).replace(microsecond=0)
    ts = to_persian_digits(now.strftime("%H:%M، %Y-%m-%d (UTC)"))

    session = requests.Session()
    session.headers.update({"User-Agent": "Mahdiz-BrokenSectionRedirects/1.0"})

    candidates = fetch_candidates()
    grouped = defaultdict(list)
    for r in candidates:
        grouped[r["target"]].append(r)

    broken = []
    html_id_cache: dict[str, set[str]] = {}
    wtxt_cache: dict[str, str] = {}

    for target, rows in grouped.items():
        anchors = api_get_sections(target, session)
        time.sleep(REQUEST_SLEEP)

        need_extra = False
        staged = []

        for r in rows:
            frag = normalize_fragment(r["fragment"])
            r["fragment_display"] = frag.replace("_", " ")

            if anchors and fragment_matches(frag, anchors):
                staged.append((r, frag, True))
            else:
                need_extra = True
                staged.append((r, frag, False))

        id_index = set()
        wtxt = ""
        if need_extra:
            if target not in html_id_cache:
                html = fetch_html(target, session)
                html_id_cache[target] = build_id_index_from_html(html)
                time.sleep(REQUEST_SLEEP)
            id_index = html_id_cache[target]

            if target not in wtxt_cache:
                wtxt_cache[target] = fetch_wikitext(target, session)
                time.sleep(REQUEST_SLEEP)
            wtxt = wtxt_cache[target]

        for r, frag, ok_by_sections in staged:
            if ok_by_sections:
                continue

            is_ok = False
            if fragment_exists_in_html_index(frag, id_index):
                is_ok = True
            elif fragment_exists_in_wikitext(wtxt, frag):
                is_ok = True

            if not is_ok:
                broken.append(r)

    for r in broken:
        r["incoming"] = api_count_backlinks(
            r["redirect"], session, namespace=BACKLINKS_NAMESPACE, max_pages=BACKLINKS_MAX_PAGES
        )
        time.sleep(REQUEST_SLEEP)

    broken.sort(key=lambda x: x["incoming"], reverse=True)
    broken = broken[:MAX_ROWS]

    table = build_table(broken)

    text = (
        "این صفحه فهرستی از تغییرمسیرهای شکسته به بخش‌ها در ویکی‌پدیای فارسی است.\n"
        "تغییرمسیر شکسته به تغییرمسیری گفته می‌شود که به بخشی ناموجود از صفحه‌ای دیگر پیوند دارد.\n"
        "ممکن است آن بخش حذف شده باشد یا املای آن عوض شده باشد یا ساختار صفحهٔ مقصد تغییر کرده باشد. ~~~~\n\n"
        "==گزارش==\n"
        f"زمان داده‌ها: <onlyinclude>{ts}</onlyinclude>.\n\n"
        f"{table}\n"
    )

    site = pywikibot.Site("fa", "wikipedia")

    page = pywikibot.Page(site, TARGET_PAGE)
    page.text = text
    page.save(summary="به‌روزرسانی خودکار گزارش تغییرمسیرهای از کار افتاده به بخش‌ها")

    sign = pywikibot.Page(site, SIGN_PAGE)
    sign.text = "~~~~~"
    sign.save(summary="به‌روزرسانی امضای گزارش دیتابیس")


if __name__ == "__main__":
    main()

