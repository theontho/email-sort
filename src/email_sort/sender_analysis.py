import email.utils
import math
from collections import Counter
from datetime import datetime, timezone

from email_sort.config import get_setting
from email_sort.db import EMAIL_TABLE, get_db


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
            pass
        except Exception:
            return None
    if parsed and parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _addresses_contain_domain(value: str, domain: str) -> bool:
    addresses = [addr.lower() for _, addr in email.utils.getaddresses([value])]
    return any(addr.endswith(f"@{domain}") for addr in addresses)


def _entropy(values: list[int]) -> float:
    if not values:
        return 0.0
    counts = Counter(values)
    total = len(values)
    return -sum((count / total) * math.log2(count / total) for count in counts.values())


def _weeks_active(dates: list[datetime]) -> float:
    if not dates:
        return 1.0
    seconds = (max(dates) - min(dates)).total_seconds()
    return max(seconds / 604800, 1.0)


def _has_user_reply(sender_domain: str, rows: list[dict]) -> bool:
    if not sender_domain:
        return False
    my_domains = [str(domain).lower() for domain in get_setting("my_domains", [])]
    for row in rows:
        to_address = (row.get("to_address") or "").lower()
        if _addresses_contain_domain(to_address, sender_domain):
            return True
        if row.get("sender_domain") in my_domains and _addresses_contain_domain(
            to_address, sender_domain
        ):
            return True
    return False


def _burst_count(dates: list[datetime]) -> int:
    sorted_dates = sorted(dates)
    bursts = 0
    left = 0
    for right, current in enumerate(sorted_dates):
        while (current - sorted_dates[left]).total_seconds() > 3600:
            left += 1
        if right - left + 1 >= 3:
            bursts += 1
            left = right + 1
    return bursts


def _compute(scope: str, key: str, rows: list[dict]) -> dict:
    dates = [parsed for row in rows if (parsed := _parse_date(row.get("date")))]
    hours = [date.hour for date in dates]
    sender_domain = rows[0].get("sender_domain") or (key if scope == "domain" else "")
    total = len(rows)
    spam_count = sum(1 for row in rows if row.get("category") == "Spam")
    promo_count = sum(1 for row in rows if row.get("category") == "Promotional")
    dmarc_failures = sum(1 for row in rows if row.get("dmarc_fail"))
    weekdays = [date.weekday() for date in dates]

    return {
        "sender": key if scope == "sender" else f"@{key}",
        "sender_domain": sender_domain,
        "scope": scope,
        "total_emails": total,
        "spam_ratio": spam_count / total if total else 0.0,
        "promotional_ratio": promo_count / total if total else 0.0,
        "avg_emails_per_week": total / _weeks_active(dates),
        "dmarc_failure_rate": dmarc_failures / total if total else 0.0,
        "has_user_reply": int(_has_user_reply(sender_domain, rows)),
        "send_hour_entropy": _entropy(hours),
        "weekday_only": int(bool(weekdays) and all(day < 5 for day in weekdays)),
        "burst_count": _burst_count(dates),
        "first_seen": min(dates).isoformat() if dates else None,
        "last_seen": max(dates).isoformat() if dates else None,
    }


def _load_rows(cursor) -> list[dict]:
    rows: list[dict] = []
    cursor.execute(
        f"""
        SELECT sender, sender_domain, to_address, subject, date,
               COALESCE(category, heuristic_category) AS category, dmarc_fail
        FROM {EMAIL_TABLE}
        WHERE sender IS NOT NULL AND sender != ''
        """
    )
    rows.extend(dict(row) for row in cursor.fetchall())
    return rows


def _save(cursor, stats: dict) -> None:
    cursor.execute(
        """
        INSERT INTO sender_stats (
            sender, sender_domain, scope, total_emails, spam_ratio, promotional_ratio,
            avg_emails_per_week, dmarc_failure_rate, has_user_reply, send_hour_entropy,
            weekday_only, burst_count, first_seen, last_seen, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(sender) DO UPDATE SET
            sender_domain = excluded.sender_domain,
            scope = excluded.scope,
            total_emails = excluded.total_emails,
            spam_ratio = excluded.spam_ratio,
            promotional_ratio = excluded.promotional_ratio,
            avg_emails_per_week = excluded.avg_emails_per_week,
            dmarc_failure_rate = excluded.dmarc_failure_rate,
            has_user_reply = excluded.has_user_reply,
            send_hour_entropy = excluded.send_hour_entropy,
            weekday_only = excluded.weekday_only,
            burst_count = excluded.burst_count,
            first_seen = excluded.first_seen,
            last_seen = excluded.last_seen,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            stats["sender"],
            stats["sender_domain"],
            stats["scope"],
            stats["total_emails"],
            stats["spam_ratio"],
            stats["promotional_ratio"],
            stats["avg_emails_per_week"],
            stats["dmarc_failure_rate"],
            stats["has_user_reply"],
            stats["send_hour_entropy"],
            stats["weekday_only"],
            stats["burst_count"],
            stats["first_seen"],
            stats["last_seen"],
        ),
    )


def analyze_all_senders() -> dict:
    conn = get_db()
    try:
        cursor = conn.cursor()
        rows = _load_rows(cursor)
        by_sender: dict[str, list[dict]] = {}
        by_domain: dict[str, list[dict]] = {}
        for row in rows:
            sender = (row.get("sender") or "").lower()
            domain = (row.get("sender_domain") or "").lower()
            if sender:
                by_sender.setdefault(sender, []).append(row)
            if domain:
                by_domain.setdefault(domain, []).append(row)

        cursor.execute("DELETE FROM sender_stats")
        for sender, grouped_rows in by_sender.items():
            _save(cursor, _compute("sender", sender, grouped_rows))
        for domain, grouped_rows in by_domain.items():
            _save(cursor, _compute("domain", domain, grouped_rows))
        conn.commit()

        cursor.execute(
            """
            SELECT sender, total_emails, spam_ratio
            FROM sender_stats
            ORDER BY spam_ratio DESC, total_emails DESC
            LIMIT 10
            """
        )
        top_spam = [dict(row) for row in cursor.fetchall()]
        return {
            "total_senders": len(by_sender),
            "total_domains": len(by_domain),
            "top_spam": top_spam,
        }
    finally:
        conn.close()


def apply_has_user_reply_prefilter(source: str | None = None) -> int:
    conn = get_db()
    try:
        cursor = conn.cursor()
        source_filter = "AND source = ?" if source else ""
        params = (source,) if source else ()
        cursor.execute(
            f"""
            UPDATE {EMAIL_TABLE}
            SET category = 'Personal', action = 'Mandatory', confidence = COALESCE(confidence, 1.0)
            WHERE (category IS NULL OR category = '')
              {source_filter}
              AND EXISTS (
                  SELECT 1 FROM sender_stats s
                  WHERE s.has_user_reply = 1
                    AND (s.sender = lower({EMAIL_TABLE}.sender) OR s.sender = '@' || lower({EMAIL_TABLE}.sender_domain))
              )
            """,
            params,
        )
        changed = cursor.rowcount
        conn.commit()
        return changed
    finally:
        conn.close()
