PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS receipts (
    id TEXT PRIMARY KEY,
    creator_user_id TEXT NOT NULL,
    creator_username TEXT NOT NULL,
    creator_display_name TEXT NOT NULL,
    guild_id TEXT NULL,
    channel_id TEXT NOT NULL,
    total_sale INTEGER NOT NULL,
    procurement_cost INTEGER NOT NULL,
    profit INTEGER NOT NULL,
    status TEXT NOT NULL,
    payment_proof_path TEXT NULL,
    payment_proof_source_url TEXT NULL,
    admin_note TEXT NULL,
    finalized_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS receipt_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id TEXT NOT NULL,
    item_name TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_sale_price INTEGER NOT NULL,
    unit_cost INTEGER NOT NULL,
    pricing_source TEXT NOT NULL,
    line_sale_total INTEGER NOT NULL,
    line_cost_total INTEGER NOT NULL,
    FOREIGN KEY (receipt_id) REFERENCES receipts(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_user_id TEXT NOT NULL,
    actor_display_name TEXT NOT NULL,
    action TEXT NOT NULL,
    target_receipt_id TEXT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
