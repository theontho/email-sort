CREATE TABLE IF NOT EXISTS sender_stats (
    sender TEXT PRIMARY KEY,
    sender_domain TEXT,
    scope TEXT DEFAULT 'sender',
    total_emails INTEGER NOT NULL DEFAULT 0,
    spam_ratio REAL NOT NULL DEFAULT 0,
    promotional_ratio REAL NOT NULL DEFAULT 0,
    avg_emails_per_week REAL NOT NULL DEFAULT 0,
    dmarc_failure_rate REAL NOT NULL DEFAULT 0,
    has_user_reply BOOLEAN NOT NULL DEFAULT 0,
    send_hour_entropy REAL NOT NULL DEFAULT 0,
    weekday_only BOOLEAN NOT NULL DEFAULT 0,
    burst_count INTEGER NOT NULL DEFAULT 0,
    first_seen TEXT,
    last_seen TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
