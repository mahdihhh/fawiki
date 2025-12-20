#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Toolforge database report bot
# Report page: ویکی‌پدیا:گزارش دیتابیس/مقاله‌های مهم ایجادنشده بر پایه حجم
#
# Author: Mahdiz
# Bot: Mahdibot
# Repository: https://github.com/mahdihhh
# License: MIT


from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Iterable, List, Tuple, Union

import toolforge
import pywikibot

LIMIT = 500

REPORT_PAGE = "ویکی‌پدیا:گزارش دیتابیس/مقاله‌های مهم ایجادنشده بر پایه حجم"
SIGN_PAGE = REPORT_PAGE + "/امضا"

PRETTY_TITLE = True

SQL = f"""
SELECT p.page_title, p.page_len
FROM page p
LEFT JOIN langlinks ll
  ON ll.ll_from = p.page_id AND ll.ll_lang = 'fa'
WHERE p.page_namespace = 0
  AND p.page_is_redirect = 0
  AND ll.ll_from IS NULL
ORDER BY p.page_len DESC
LIMIT {LIMIT};
"""

PERSIAN_DIGITS = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")

def fa_number(n: int) -> str:
    # 427514 -> ۴۲۷٬۵۱۴
    s = f"{n:,}".replace(",", "٬")
    return s.translate(PERSIAN_DIGITS)

def normalize_title(t: Union[str, bytes, bytearray]) -> str:
    # DB may return bytes
    if isinstance(t, (bytes, bytearray)):
        t = t.decode("utf-8", "replace")
    # enwiki DB uses underscores
    if PRETTY_TITLE:
        t = t.replace("_", " ")
    return t.strip()

def en_interwiki_link(title: str) -> str:
    # [[:en:Title]] makes a link to English Wikipedia from fawiki
    return f"[[:en:{title}]]"

def require_toolforge_env_hint() -> None:
    
    cnf = os.path.expanduser("~/replica.my.cnf")
    if not os.path.exists(cnf):
        # Don't hard-fail; just warn to stderr for logs
        print("⚠️  هشدار: فایل ~/replica.my.cnf پیدا نشد. اگر بیرون Toolforge هستی، اتصال replica ممکنه fail بشه.", file=sys.stderr)

def fetch_rows() -> List[Tuple[str, int]]:
    require_toolforge_env_hint()
    conn = toolforge.connect("enwiki")
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
            out: List[Tuple[str, int]] = []
            for (title, page_len) in cur.fetchall():
                title_str = normalize_title(title)
                out.append((title_str, int(page_len)))
            return out
    finally:
        conn.close()

def build_report_text(rows: Iterable[Tuple[str, int]]) -> str:
    lines: List[str] = []

    lines.append("این فهرست طولانی‌ترین مقاله‌های ویکی‌پدیای انگلیسی را نشان می‌دهد که در ویکی‌پدیای فارسی معادل ندارند (یا دست‌کم پیوند میان‌ویکی داده نشده‌است).")
    lines.append("~~~~")
    lines.append("")
    lines.append('{| class="wikitable sortable"')
    lines.append("! ردیف !! مقاله (en) !! حجم (بایت)")

    for i, (title, page_len) in enumerate(rows, start=1):
        link = en_interwiki_link(title)
        lines.append("|-")
        lines.append(f"| {fa_number(i)} || {link} || {fa_number(page_len)}")

    lines.append("|}")
    lines.append("")

    return "\n".join(lines)

def update_signature_page(site: pywikibot.Site) -> None:
    sign = pywikibot.Page(site, SIGN_PAGE)
    sign.text = "~~~~~"
    sign.save(summary="به‌روزرسانی خودکار امضا ", minor=True, bot=True)

def update_report_page(site: pywikibot.Site, text: str) -> None:
    page = pywikibot.Page(site, REPORT_PAGE)
    page.text = text
    page.save(summary="به‌روزرسانی خودکار گزارش (مقاله‌های مهم ایجادنشده بر پایه حجم)", minor=False, bot=True)

def main() -> int:
    try:
        rows = fetch_rows()
        print(f"✅ rows fetched: {len(rows)}", file=sys.stderr)

        site = pywikibot.Site("fa", "wikipedia")

        # 1) update signature page first (so transclusion shows fresh timestamp)
        update_signature_page(site)
        print("✅ signature page updated", file=sys.stderr)

        # 2) update main report
        report_text = build_report_text(rows)
        update_report_page(site, report_text)
        print("✅ report page updated", file=sys.stderr)

        return 0

    except Exception as e:
        # Make logs useful for Toolforge jobs logs
        print("⛔ ERROR:", repr(e), file=sys.stderr)
        raise

if __name__ == "__main__":
    raise SystemExit(main())
