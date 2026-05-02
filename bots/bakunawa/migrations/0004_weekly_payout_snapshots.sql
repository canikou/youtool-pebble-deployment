PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS weekly_payout_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_key TEXT NOT NULL UNIQUE,
    week_start_at TEXT NOT NULL,
    week_end_at TEXT NOT NULL,
    ranking_basis TEXT NOT NULL,
    top_mech_user_id TEXT NULL,
    runner_up_user_id TEXT NULL,
    total_sales INTEGER NOT NULL DEFAULT 0,
    total_profit INTEGER NOT NULL DEFAULT 0,
    receipt_count INTEGER NOT NULL DEFAULT 0,
    actor_user_id TEXT NOT NULL,
    actor_display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weekly_payout_snapshot_receipts (
    snapshot_id INTEGER NOT NULL,
    receipt_id TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, receipt_id),
    UNIQUE (receipt_id),
    FOREIGN KEY (snapshot_id) REFERENCES weekly_payout_snapshots(id) ON DELETE CASCADE,
    FOREIGN KEY (receipt_id) REFERENCES receipts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_weekly_payout_snapshot_receipts_receipt
    ON weekly_payout_snapshot_receipts(receipt_id);

CREATE INDEX IF NOT EXISTS idx_weekly_payout_snapshot_receipts_snapshot
    ON weekly_payout_snapshot_receipts(snapshot_id, receipt_id);

CREATE TABLE IF NOT EXISTS weekly_payout_snapshot_adjustments (
    snapshot_id INTEGER NOT NULL,
    payout_adjustment_id INTEGER NOT NULL UNIQUE,
    PRIMARY KEY (snapshot_id, payout_adjustment_id),
    FOREIGN KEY (snapshot_id) REFERENCES weekly_payout_snapshots(id) ON DELETE CASCADE,
    FOREIGN KEY (payout_adjustment_id) REFERENCES payout_adjustments(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_weekly_payout_snapshot_adjustments_snapshot
    ON weekly_payout_snapshot_adjustments(snapshot_id, payout_adjustment_id);
