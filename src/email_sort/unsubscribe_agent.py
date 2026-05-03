import asyncio
import json
import random
import re
import smtplib
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from email_sort.config import get_config_dir, get_section_setting
from email_sort.db import EMAIL_TABLES, get_db


PROOF_DIR = get_config_dir() / "unsubscribe_proofs"
CLICK_TEXT = ("unsubscribe", "confirm", "opt out", "remove")


def _extract_urls(list_unsubscribe: str | None) -> tuple[list[str], list[str]]:
    if not list_unsubscribe:
        return [], []
    values = re.findall(r"<([^>]+)>", list_unsubscribe) or re.split(r"\s*,\s*", list_unsubscribe)
    http_urls = [value.strip() for value in values if value.strip().lower().startswith(("http://", "https://"))]
    mailtos = [value.strip() for value in values if value.strip().lower().startswith("mailto:")]
    return http_urls, mailtos


def extract_unsubscribe_urls_from_html(html: str | None) -> list[str]:
    if not html:
        return []
    urls: list[str] = []
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ").lower()
        href = str(anchor["href"])
        if href.startswith("http") and ("unsubscribe" in href.lower() or "unsubscribe" in text or "opt out" in text):
            urls.append(href)
    return urls[:5]


def _safe_sender(sender: str) -> bool:
    sender_lower = (sender or "").lower()
    patterns = [str(item).lower() for item in get_section_setting("unsubscribe", "safe_senders", [])]
    return any(re.search(pattern, sender_lower) for pattern in patterns)


def _rate_limits() -> tuple[int, int]:
    return (
        int(get_section_setting("unsubscribe", "max_per_hour", 10)),
        int(get_section_setting("unsubscribe", "max_per_day", 50)),
    )


def _check_rate_limit(cursor) -> tuple[bool, str | None]:
    max_hour, max_day = _rate_limits()
    hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
    day_ago = (datetime.now() - timedelta(days=1)).isoformat()
    cursor.execute("SELECT COUNT(*) FROM unsubscribe_log WHERE attempted_at >= ?", (hour_ago,))
    hour_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM unsubscribe_log WHERE attempted_at >= ?", (day_ago,))
    day_count = cursor.fetchone()[0]
    if hour_count >= max_hour:
        return False, f"hourly rate limit reached ({hour_count}/{max_hour})"
    if day_count >= max_day:
        return False, f"daily rate limit reached ({day_count}/{max_day})"
    return True, None


def _log(sender, url, method, status, screenshot_path=None, error_message=None) -> None:
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO unsubscribe_log (sender, url, method, status, screenshot_path, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sender, url, method, status, screenshot_path, error_message),
        )
        conn.commit()
    finally:
        conn.close()


def _request_with_backoff(method: str, url: str, **kwargs) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = requests.request(method, url, timeout=20, **kwargs)
            if response.status_code not in {429, 500, 502, 503, 504}:
                return response
            last_error = RuntimeError(f"HTTP {response.status_code}")
        except requests.RequestException as exc:
            last_error = exc
        time.sleep((2**attempt) + random.random())
    raise RuntimeError(str(last_error))


def _rfc8058_post(url: str) -> tuple[bool, str | None]:
    try:
        response = _request_with_backoff(
            "POST",
            url,
            data="List-Unsubscribe=One-Click",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        return response.status_code < 300, f"HTTP {response.status_code}"
    except Exception as exc:
        return False, str(exc)


def _http_get(url: str) -> tuple[bool, str | None]:
    try:
        response = _request_with_backoff("GET", url, allow_redirects=True)
        return response.status_code < 400, f"HTTP {response.status_code}"
    except Exception as exc:
        return False, str(exc)


def _mailto(mailto: str) -> tuple[bool, str | None]:
    smtp_host = get_section_setting("smtp", "host")
    smtp_username = get_section_setting("smtp", "username")
    smtp_password = get_section_setting("smtp", "password")
    if not smtp_host or not smtp_username or not smtp_password:
        return False, "mailto fallback requires [smtp] host, username, password"
    parsed = urlparse(mailto)
    recipient = unquote(parsed.path)
    message = EmailMessage()
    message["From"] = smtp_username
    message["To"] = recipient
    message["Subject"] = "Unsubscribe"
    message.set_content("Please unsubscribe this address from future mailings.")
    try:
        with smtplib.SMTP_SSL(smtp_host, int(get_section_setting("smtp", "port", 465))) as smtp:
            smtp.login(smtp_username, smtp_password)
            smtp.send_message(message)
        return True, None
    except Exception as exc:
        return False, str(exc)


async def browser_unsubscribe(url: str, sender_domain: str) -> tuple[bool, str | None, str | None]:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        return False, f"Playwright unavailable: {exc}", None

    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    safe_domain = re.sub(r"[^A-Za-z0-9_.-]", "_", sender_domain or "unknown")
    screenshot_path = PROOF_DIR / f"{safe_domain}_{int(time.time())}.png"
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until="networkidle", timeout=45000)
            clicked = False
            for _ in range(2):
                target = None
                for selector in ("button", "input[type=submit]", "a"):
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        text = (await element.inner_text() if selector != "input[type=submit]" else await element.get_attribute("value")) or ""
                        if any(term in text.lower() for term in CLICK_TEXT):
                            target = element
                            break
                    if target:
                        break
                if not target:
                    forms = await page.query_selector_all("form button, form input[type=submit]")
                    target = forms[0] if forms else None
                if not target:
                    break
                await target.click()
                clicked = True
                try:
                    await page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:
                    pass
            await page.screenshot(path=str(screenshot_path), full_page=True)
            await browser.close()
        return clicked, None if clicked else "no confirmation element found", str(screenshot_path)
    except Exception as exc:
        return False, str(exc), None


def unsubscribe_candidates(limit: int | None = None) -> list[dict]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        candidates: dict[str, dict] = {}
        for table_name in EMAIL_TABLES:
            query = f"""
                SELECT sender, sender_domain, list_unsubscribe, list_unsubscribe_post,
                       body_unsubscribe_links, body_html, category, is_digest
                FROM {table_name}
                WHERE sender IS NOT NULL AND sender != ''
                  AND (list_unsubscribe IS NOT NULL OR body_unsubscribe_links IS NOT NULL OR body_html IS NOT NULL)
                  AND (category IN ('Promotional','Newsletter','Spam','Social','Shopping','Tech','Health') OR is_digest = 1)
                ORDER BY is_digest DESC, date DESC
            """
            if limit:
                query += f" LIMIT {int(limit)}"
            cursor.execute(query)
            for row in cursor.fetchall():
                sender = row["sender"]
                if sender not in candidates and not _safe_sender(sender):
                    candidates[sender] = dict(row)
        return list(candidates.values())
    finally:
        conn.close()


async def process_candidate(candidate: dict) -> dict:
    sender = candidate["sender"]
    http_urls, mailtos = _extract_urls(candidate.get("list_unsubscribe"))
    body_urls = []
    if candidate.get("body_unsubscribe_links"):
        try:
            body_urls = json.loads(candidate["body_unsubscribe_links"])
        except Exception:
            body_urls = []
    body_urls.extend(extract_unsubscribe_urls_from_html(candidate.get("body_html")))
    attempts: list[tuple[str, str]] = []
    if "List-Unsubscribe=One-Click" in (candidate.get("list_unsubscribe_post") or ""):
        attempts.extend(("rfc8058_post", url) for url in http_urls)
    attempts.extend(("http_get", url) for url in http_urls)
    attempts.extend(("browser_agent", url) for url in http_urls)
    attempts.extend(("browser_agent", url) for url in body_urls if str(url).startswith("http"))
    attempts.extend(("mailto", mailto) for mailto in mailtos)

    errors = []
    for method, url in attempts:
        conn = get_db()
        try:
            ok, reason = _check_rate_limit(conn.cursor())
        finally:
            conn.close()
        if not ok:
            return {"sender": sender, "status": "failed", "error": reason}
        screenshot_path = None
        if method == "rfc8058_post":
            success, message = _rfc8058_post(url)
        elif method == "http_get":
            success, message = _http_get(url)
        elif method == "browser_agent":
            success, message, screenshot_path = await browser_unsubscribe(url, candidate.get("sender_domain") or "")
        else:
            success, message = _mailto(url)
        _log(sender, url, method, "success" if success else "failed", screenshot_path, message)
        if success:
            return {"sender": sender, "status": "success", "method": method, "url": url}
        errors.append(f"{method} {url}: {message}")
    if not attempts:
        _log(sender, "", "browser_agent", "needs_review", None, "no unsubscribe target found")
    return {"sender": sender, "status": "failed", "error": "; ".join(errors) or "no unsubscribe target found"}


async def process_unsubscribe_list(dry_run: bool = True, execute: bool = False, yes: bool = False) -> dict:
    candidates = unsubscribe_candidates()
    if dry_run or not execute:
        return {"dry_run": True, "total_found": len(candidates), "sample": candidates[:20]}
    if not yes:
        print(f"About to attempt unsubscribe for {len(candidates)} senders.")
        answer = input("Continue? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            return {"cancelled": True, "total_found": len(candidates)}
    results = []
    for candidate in candidates:
        results.append(await process_candidate(candidate))
    return {
        "attempted": len(results),
        "successful": [result for result in results if result["status"] == "success"],
        "failed": [result for result in results if result["status"] != "success"],
    }


if __name__ == "__main__":
    asyncio.run(process_unsubscribe_list(dry_run=True))
