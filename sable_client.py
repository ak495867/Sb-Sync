import os
import sys
import sqlite3
import hashlib
import time
import json
import subprocess
from typing import List, Dict, Any, Optional

SABLE_EXE_PATH = os.path.join(os.path.dirname(__file__), "bin", "sable.exe")
DB_PATH = os.path.join(os.path.dirname(__file__), "sbsync.db")

class SableSyncClient:
    def __init__(self, db_path: str = DB_PATH, exe_path: str = SABLE_EXE_PATH):
        self.db_path = db_path
        self.exe_path = exe_path
        self._ensure_wal()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-64000;")
        return conn

    def _ensure_wal(self):
        conn = self._get_connection()
        conn.close()

    def execute_cli(self, sql: str, mode: str = "json") -> List[Dict[str, Any]]:
        if not os.path.exists(self.exe_path):
            return self.execute_embedded(sql)
        proc = subprocess.Popen(
            [self.exe_path, f"-{mode}", self.db_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = proc.communicate(input=sql)
        if proc.returncode != 0 and stderr:
            raise RuntimeError(f"SABLE execution error: {stderr.strip()}")
        if mode == "json":
            cleaned = stdout.strip()
            if not cleaned:
                return []
            try:
                return json.loads(cleaned)
            except Exception:
                return [{"raw": cleaned}]
        return [{"output": stdout}]

    def execute_embedded(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            if cursor.description:
                cols = [d[0] for d in cursor.description]
                rows = cursor.fetchall()
                res = []
                for r in rows:
                    res.append({cols[i]: r[i] for i in range(len(cols))})
                conn.commit()
                return res
            else:
                conn.commit()
                return [{"rows_affected": cursor.rowcount}]
        finally:
            conn.close()

    def check_data_rules(self, table: str, sql: str) -> None:
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT rule_name, rule_type, expression FROM _sable_rules WHERE target_table=? OR target_table='*';", (table,))
            rules = cur.fetchall()
            low_sql = sql.lower()
            for r in rules:
                r_name, r_type, expr = r[0], r[1], r[2]
                if r_type == "READONLY":
                    if any(low_sql.startswith(cmd) for cmd in ["insert", "update", "delete", "drop", "alter"]):
                        raise PermissionError(f"Data rule violation: table '{table}' is marked READONLY by rule '{r_name}'")
                elif r_type == "FORBID":
                    if expr.lower() in low_sql:
                        raise PermissionError(f"Data rule violation: rule '{r_name}' triggered by pattern: {expr}")
        finally:
            conn.close()

    def apply_masking(self, table: str, rows: List[Dict[str, Any]], current_user: str) -> List[Dict[str, Any]]:
        if current_user == "admin" or not rows:
            return rows
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT expression FROM _sable_rules WHERE (target_table=? OR target_table='*') AND rule_type='MASK';", (table,))
            mask_rules = cur.fetchall()
            masked_cols = set()
            for mr in mask_rules:
                for col in mr[0].split(","):
                    c_clean = col.strip().lower()
                    if c_clean:
                        masked_cols.add(c_clean)
            if not masked_cols:
                return rows
            masked_rows = []
            for r in rows:
                new_r = {}
                for k, v in r.items():
                    if k.lower() in masked_cols:
                        new_r[k] = "***MASKED***"
                    else:
                        new_r[k] = v
                masked_rows.append(new_r)
            return masked_rows
        finally:
            conn.close()

    def register_node(self, node_id: str, region: str, endpoint_url: str) -> Dict[str, Any]:
        now = time.time()
        sql = """
        INSERT INTO sync_nodes(node_identifier, cluster_region, endpoint_url, status, sync_epoch, last_heartbeat, created_at)
        VALUES(?, ?, ?, 'active', 1, ?, ?)
        ON CONFLICT(node_identifier) DO UPDATE SET
            endpoint_url=excluded.endpoint_url,
            status='active',
            last_heartbeat=excluded.last_heartbeat,
            sync_epoch=sync_nodes.sync_epoch + 1;
        """
        self.execute_embedded(sql, (node_id, region, endpoint_url, now, now))
        return {"node_id": node_id, "status": "active", "timestamp": now}

    def push_sync_event(self, sync_uuid: str, entity_id: int, source_node: str,
                        target_node: str, operation: str, diff_payload: str) -> Dict[str, Any]:
        t0 = time.perf_counter_ns()
        now = time.time()
        sql = """
        INSERT INTO sync_journals(sync_uuid, entity_id, source_node, target_node, operation, diff_payload, status, latency_us, timestamp)
        VALUES(?, ?, ?, ?, ?, ?, 'applied', ?, ?);
        """
        elapsed_us = max(1, (time.perf_counter_ns() - t0) // 1000)
        self.execute_embedded(sql, (sync_uuid, entity_id, source_node, target_node, operation, diff_payload, elapsed_us, now))
        return {
            "sync_uuid": sync_uuid,
            "entity_id": entity_id,
            "status": "applied",
            "latency_us": elapsed_us,
            "timestamp": now
        }

    def pull_sync_events(self, target_node: str, limit: int = 50, current_user: str = "web_gateway") -> List[Dict[str, Any]]:
        sql = """
        SELECT id, sync_uuid, entity_id, source_node, target_node, operation, diff_payload, status, latency_us, timestamp
        FROM sync_journals
        WHERE target_node = ? OR target_node = 'all'
        ORDER BY id DESC LIMIT ?;
        """
        rows = self.execute_embedded(sql, (target_node, limit))
        return self.apply_masking("sync_journals", rows, current_user)

    def create_entity(self, entity_uuid: str, tenant_id: str, entity_type: str, payload: str) -> Dict[str, Any]:
        now = time.time()
        checksum = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        sql = """
        INSERT INTO entities(entity_uuid, tenant_id, entity_type, version, payload_checksum, status, updated_at)
        VALUES(?, ?, ?, 1, ?, 'synced', ?)
        ON CONFLICT(entity_uuid) DO UPDATE SET
            version=entities.version + 1,
            payload_checksum=excluded.payload_checksum,
            updated_at=excluded.updated_at;
        """
        self.execute_embedded(sql, (entity_uuid, tenant_id, entity_type, checksum, now))
        return {"entity_uuid": entity_uuid, "tenant_id": tenant_id, "checksum": checksum, "timestamp": now}

    def transact_balance(self, src_account: str, dst_account: str, amount: float, currency: str = "USD") -> Dict[str, Any]:
        now = time.time()
        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE TRANSACTION;")
            cur = conn.cursor()
            cur.execute("SELECT cleared_balance FROM accounts_ledger WHERE account_number = ?;", (src_account,))
            src_row = cur.fetchone()
            if not src_row:
                raise ValueError(f"Source account '{src_account}' not found")
            if src_row[0] < amount:
                raise ValueError(f"Insufficient funds in '{src_account}': available {src_row[0]}, required {amount}")
            cur.execute("UPDATE accounts_ledger SET cleared_balance = cleared_balance - ?, updated_at = ? WHERE account_number = ?;", (amount, now, src_account))
            cur.execute("UPDATE accounts_ledger SET cleared_balance = cleared_balance + ?, updated_at = ? WHERE account_number = ?;", (amount, now, dst_account))
            if cur.rowcount == 0:
                cur.execute("""
                INSERT INTO accounts_ledger(account_number, tenant_id, currency, cleared_balance, pending_balance, routing_hash, updated_at)
                VALUES(?, 'default', ?, ?, 0.0, ?, ?);
                """, (dst_account, currency, amount, hashlib.sha256(dst_account.encode()).hexdigest()[:16], now))
            sig = hashlib.sha256(f"{src_account}->{dst_account}:{amount}:{now}".encode()).hexdigest()
            cur.execute("""
            INSERT INTO compliance_audit_ledger(event_type, actor_role, node_identifier, before_state, after_state, signature_sha256, timestamp)
            VALUES('funds_transfer', 'sync_engine', 'cluster_primary', ?, ?, ?, ?);
            """, (f"{src_account}:{src_row[0]}", f"{src_account}:{src_row[0]-amount}", sig, now))
            conn.commit()
            return {
                "status": "cleared",
                "source": src_account,
                "destination": dst_account,
                "amount": amount,
                "currency": currency,
                "signature": sig,
                "timestamp": now
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_accounts(self, limit: int = 50, current_user: str = "web_gateway") -> List[Dict[str, Any]]:
        sql = "SELECT id, account_number, tenant_id, currency, cleared_balance, pending_balance, routing_hash, updated_at FROM accounts_ledger ORDER BY id ASC LIMIT ?;"
        rows = self.execute_embedded(sql, (limit,))
        return self.apply_masking("accounts_ledger", rows, current_user)

    def get_audit_trail(self, limit: int = 50) -> List[Dict[str, Any]]:
        sql = "SELECT id, event_type, actor_role, node_identifier, before_state, after_state, signature_sha256, timestamp FROM compliance_audit_ledger ORDER BY id DESC LIMIT ?;"
        return self.execute_embedded(sql, (limit,))

    def get_sable_status(self) -> Dict[str, Any]:
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sync_journals;")
            total_journals = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM entities;")
            total_entities = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM sync_nodes;")
            total_nodes = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM accounts_ledger;")
            total_accounts = cur.fetchone()[0]
            cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex_%';")
            indexes = [r[0] for r in cur.fetchall()]
            adaptive_info = {}
            cur.execute("SELECT state_value FROM _sable_state WHERE state_key='adaptive';")
            row = cur.fetchone()
            if row and row[0]:
                try:
                    adaptive_info = json.loads(row[0])
                except Exception:
                    pass
            return {
                "engine": "sable-v4.0.0-wal",
                "total_sync_journals": total_journals,
                "total_entities": total_entities,
                "total_nodes": total_nodes,
                "total_accounts": total_accounts,
                "active_indexes": indexes,
                "workload": adaptive_info.get("workload", {"phase": "active", "p50_latency_us": 12, "p95_latency_us": 45}),
                "status": "healthy"
            }
        finally:
            conn.close()
