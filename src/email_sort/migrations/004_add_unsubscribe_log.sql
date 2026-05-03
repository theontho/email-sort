CREATE TABLE IF NOT EXISTS unsubscribe_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sender TEXT,
    url TEXT,
    method TEXT CHECK(method IN ('rfc8058_post','http_get','mailto','browser_agent')),
    status TEXT CHECK(status IN ('success','failed','needs_review')),
    screenshot_path TEXT,
    attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS failed_unsubscribes (
    sender TEXT PRIMARY KEY,
    unsubscribed_at TEXT,
    emails_after_unsubscribe INTEGER NOT NULL,
    last_received TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
