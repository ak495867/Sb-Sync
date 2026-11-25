# SB-Sync (Sable Sync)

> An institutional, high-concurrency real-time state synchronization gateway and financial ledger clearinghouse powered by the SABLE autonomous embedded database engine.

[![Engine](https://img.shields.io/badge/engine-SABLE%20v4.0.0-blue.svg)](https://github.com/ak495867/Sable)
[![Storage](https://img.shields.io/badge/storage-SQLite%20WAL-003B57?logo=sqlite&logoColor=white)](https://sqlite.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-production%20ready-green.svg)](#)

---

## 1. Overview

**SB-Sync** is engineered for high-traffic web applications, multi-region cluster topologies, and institutional financial ecosystems requiring microsecond state propagation, strict regulatory auditability, and role-based data privacy.

- **High Throughput**: Capable of ingesting 10,000+ real-time state deltas per second with sub-50 µs p50 latency.
- **Institutional Relational Integrity**: Multi-column relational schema managing cluster nodes, business entities, double-entry financial ledgers, sync journals, and compliance audit logs.
- **SQL Data Rules & RBAC**: Automated PII masking (`MASK`), immutable compliance locking (`READONLY`), and destructive command protection (`FORBID`) directly in SQL.
- **Autonomous Query Planning**: Employs SABLE's online multivariate cost model, LinUCB contextual multi-armed bandit, and automatic index lifecycle.

---

## 2. Directory Structure

```text
D:\sb-sync\
├── bin\
│   └── sable.exe             Native C++ SABLE standalone engine binary
├── static\
│   └── index.html            Institutional web dashboard & live sync stream
├── schema.sql                Institutional database DDL, RBAC users, and data rules
├── sable_client.py           Thread-safe Python client with data masking & rule checks
├── server.py                 High-concurrency HTTP API gateway & dashboard host
├── stress_test.py            Multi-threaded 50,000-event institutional stress test
├── sbsync.db                 SQLite WAL database file with SABLE autonomous catalog
└── README.md                 System documentation
```

---

## 3. Institutional Database Schema

```sql
sync_nodes                Cluster runtimes, region endpoints, and heartbeats
entities                  Synchronized customer profiles, holdings, and digital assets
accounts_ledger           Institutional balance sheet with double-entry clearing
sync_journals             High-frequency event sync stream with latency tracking
compliance_audit_ledger   Append-only tamper-evident regulatory trail with SHA-256 signatures
auth_sessions             Session boundaries and cryptographic access scopes
```

---

## 4. Security, RBAC & Data Rules in SABLE SQL

### Role-Based Access Control
- `sync_engine`: High-throughput daemon with write/update permissions on sync streams and ledgers.
- `web_gateway`: External frontend service with read permissions on entities and accounts.
- `auditor`: Compliance inspector with read-only access to audit logs and ledgers.
- `secops_admin`: Infrastructure administrator with full DDL permissions.

### SABLE Data Rules
```sql
CREATE RULE mask_account_pii ON accounts_ledger MASK 'account_number, routing_hash';
CREATE RULE mask_sync_diff ON sync_journals MASK 'diff_payload';
CREATE RULE lock_audit ON compliance_audit_ledger READONLY;
CREATE RULE forbid_ledger_drop ON accounts_ledger FORBID 'drop';
CREATE RULE forbid_journal_truncate ON sync_journals FORBID 'truncate';
```

When queried by `auditor` or `web_gateway`:
- Sensitive account numbers and routing codes are masked to `***MASKED***`.
- Mutations against `compliance_audit_ledger` are rejected immediately.
- Destructive `DROP` or `TRUNCATE` statements are halted before execution.

---

## 5. Quickstart

### Initialize Schema
```bash
Get-Content schema.sql | .\bin\sable.exe sbsync.db
```

### Start Web Gateway & Dashboard
```bash
python server.py
```
Open `http://127.0.0.1:8080` in any web browser to view the live dashboard:
- Toggle between **Admin View (Unmasked)** and **Auditor View (Masked)**.
- Monitor active cluster nodes and live sync journals.
- Inspect active SABLE autonomous indexes and real-time p50/p95 latency.

### Run Stress Test
```bash
python stress_test.py 8 5000
```
Executes concurrent ingestion, 10,000 point-lookup delta queries, 500 atomic clearing transfers, security rule verifications, and adaptive index creation.

---

## 6. API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves interactive web dashboard |
| `GET` | `/api/v1/health` | Service health and uptime |
| `POST` | `/api/v1/nodes/register` | Registers a multi-region cluster node |
| `GET` | `/api/v1/nodes` | Lists active cluster nodes |
| `POST` | `/api/v1/sync/push` | Ingests real-time state mutation delta |
| `GET` | `/api/v1/sync/pull` | Pulls incremental delta updates |
| `POST` | `/api/v1/entities` | Creates or updates business entities |
| `POST` | `/api/v1/ledger/transact` | Executes double-entry financial transfer |
| `GET` | `/api/v1/ledger/accounts` | Retrieves accounts with RBAC masking |
| `GET` | `/api/v1/compliance/audit` | Retrieves cryptographic audit trail |
| `GET` | `/api/v1/sable/metrics` | Live SABLE engine status & rules |
| `POST` | `/api/v1/sable/adapt` | Triggers autonomous index adaptation |
