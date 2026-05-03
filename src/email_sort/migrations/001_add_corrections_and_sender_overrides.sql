CREATE TABLE IF NOT EXISTS corrections (
    message_id TEXT PRIMARY KEY,
    original_category TEXT,
    corrected_category TEXT,
    original_action TEXT,
    corrected_action TEXT,
    corrected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sender_overrides (
    sender TEXT PRIMARY KEY,
    override_category TEXT NOT NULL,
    override_action TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
