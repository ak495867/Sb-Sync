CREATE TABLE IF NOT EXISTS sync_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_identifier TEXT UNIQUE NOT NULL,
    cluster_region TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    sync_epoch INTEGER DEFAULT 0,
    last_heartbeat REAL NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_uuid TEXT UNIQUE NOT NULL,
    tenant_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    payload_checksum TEXT NOT NULL,
    status TEXT DEFAULT 'synced',
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_number TEXT UNIQUE NOT NULL,
    tenant_id TEXT NOT NULL,
    currency TEXT DEFAULT 'USD',
    cleared_balance REAL DEFAULT 0.0,
    pending_balance REAL DEFAULT 0.0,
    routing_hash TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_journals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_uuid TEXT UNIQUE NOT NULL,
    entity_id INTEGER NOT NULL,
    source_node TEXT NOT NULL,
    target_node TEXT NOT NULL,
    operation TEXT NOT NULL,
    diff_payload TEXT NOT NULL,
    status TEXT DEFAULT 'applied',
    latency_us INTEGER DEFAULT 0,
    timestamp REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS compliance_audit_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    actor_role TEXT NOT NULL,
    node_identifier TEXT NOT NULL,
    before_state TEXT,
    after_state TEXT,
    signature_sha256 TEXT NOT NULL,
    timestamp REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    token_hash TEXT UNIQUE NOT NULL,
    expires_at REAL NOT NULL,
    scopes TEXT NOT NULL
);

CREATE USER sync_engine IDENTIFIED BY 'sync_secret_2026' ROLE 'engine';
CREATE USER web_gateway IDENTIFIED BY 'gateway_key_2026' ROLE 'gateway';
CREATE USER auditor IDENTIFIED BY 'audit_pass_2026' ROLE 'auditor';

GRANT INSERT, UPDATE, SELECT ON sync_journals TO sync_engine;
GRANT INSERT, UPDATE, SELECT ON entities TO sync_engine;
GRANT INSERT, UPDATE, SELECT ON accounts_ledger TO sync_engine;
GRANT INSERT, SELECT ON compliance_audit_ledger TO sync_engine;
GRANT INSERT, UPDATE, SELECT ON sync_nodes TO sync_engine;

GRANT SELECT, INSERT ON sync_journals TO web_gateway;
GRANT SELECT ON entities TO web_gateway;
GRANT SELECT ON accounts_ledger TO web_gateway;
GRANT SELECT ON sync_nodes TO web_gateway;

GRANT SELECT ON compliance_audit_ledger TO auditor;
GRANT SELECT ON accounts_ledger TO auditor;
GRANT SELECT ON sync_journals TO auditor;
GRANT SELECT ON entities TO auditor;

CREATE RULE mask_account_pii ON accounts_ledger MASK 'account_number, routing_hash';
CREATE RULE mask_sync_diff ON sync_journals MASK 'diff_payload';
CREATE RULE lock_audit_updates ON compliance_audit_ledger FORBID 'update';
CREATE RULE lock_audit_deletes ON compliance_audit_ledger FORBID 'delete';
CREATE RULE lock_audit_drops ON compliance_audit_ledger FORBID 'drop';
CREATE RULE forbid_ledger_drop ON accounts_ledger FORBID 'drop';
CREATE RULE forbid_journal_truncate ON sync_journals FORBID 'truncate';
