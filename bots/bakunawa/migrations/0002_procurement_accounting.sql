PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS procurement_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    cutover_at TEXT NULL,
    actor_user_id TEXT NULL,
    actor_display_name TEXT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipt_accounting (
    receipt_id TEXT PRIMARY KEY,
    policy TEXT NOT NULL,
    recorded_by_user_id TEXT NOT NULL,
    recorded_by_display_name TEXT NOT NULL,
    recorded_for_user_id TEXT NOT NULL,
    recorded_for_display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (receipt_id) REFERENCES receipts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_receipt_accounting_policy
    ON receipt_accounting(policy);
CREATE INDEX IF NOT EXISTS idx_receipt_accounting_recorded_for
    ON receipt_accounting(recorded_for_user_id);

CREATE TABLE IF NOT EXISTS procurement_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    amount INTEGER NOT NULL CHECK (amount <> 0),
    reason TEXT NOT NULL,
    receipt_id TEXT NULL,
    actor_user_id TEXT NOT NULL,
    actor_display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (receipt_id) REFERENCES receipts(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_procurement_ledger_user_created_at
    ON procurement_ledger(user_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_procurement_ledger_receipt_id
    ON procurement_ledger(receipt_id);
