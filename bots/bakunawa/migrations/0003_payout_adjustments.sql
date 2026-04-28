PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS payout_adjustments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    amount_cents INTEGER NOT NULL CHECK (amount_cents <> 0),
    reason TEXT NOT NULL,
    source_user_id TEXT NULL,
    source_display_name TEXT NULL,
    actor_user_id TEXT NOT NULL,
    actor_display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    settled_at TEXT NULL,
    settled_by_user_id TEXT NULL,
    settled_by_display_name TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_payout_adjustments_open_user_created
    ON payout_adjustments(user_id, settled_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_payout_adjustments_open_created
    ON payout_adjustments(settled_at, created_at, id);
