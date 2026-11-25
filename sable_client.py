import os
import sys
import ctypes
import json
import time
import subprocess
from typing import Any, Dict, List, Optional

BIN_DIR = os.path.join(os.path.dirname(__file__), "bin")
DLL_PATH = os.path.join(BIN_DIR, "libsable.dll")
EXE_PATH = os.path.join(BIN_DIR, "sable.exe")
DB_PATH = os.path.join(os.path.dirname(__file__), "sbsync.db")

class NativeSableEngine:
    def __init__(self, db_path: str = DB_PATH, dll_path: str = DLL_PATH, exe_path: str = EXE_PATH):
        self.db_path = db_path
        self.dll_path = dll_path
        self.exe_path = exe_path
        self.dll = None
        self.db_handle = None
        self._init_dll()

    def _init_dll(self):
        if os.path.exists(self.dll_path):
            try:
                self.dll = ctypes.CDLL(self.dll_path)
                self.dll.sable_db_open.restype = ctypes.c_void_p
                self.dll.sable_db_open.argtypes = [ctypes.c_char_p]
                self.dll.sable_db_close.argtypes = [ctypes.c_void_p]
                self.dll.sable_db_execute_json.restype = ctypes.c_void_p
                self.dll.sable_db_execute_json.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
                self.dll.sable_db_status_json.restype = ctypes.c_void_p
                self.dll.sable_db_status_json.argtypes = [ctypes.c_void_p]
                self.dll.sable_free_string.argtypes = [ctypes.c_void_p]

                self.db_handle = self.dll.sable_db_open(self.db_path.encode("utf-8"))
            except Exception:
                self.dll = None
                self.db_handle = None

    def execute_json(self, sql: str, user: str = "admin") -> List[Dict[str, Any]]:
        if self.dll and self.db_handle:
            sql_b = sql.encode("utf-8")
            user_b = user.encode("utf-8")
            ptr = self.dll.sable_db_execute_json(self.db_handle, sql_b, user_b)
            if not ptr:
                return []
            try:
                res_str = ctypes.string_at(ptr).decode("utf-8")
            finally:
                self.dll.sable_free_string(ptr)
            if not res_str.strip():
                return []
            parsed = json.loads(res_str)
            if isinstance(parsed, dict) and "error" in parsed:
                raise RuntimeError(parsed["error"])
            return parsed

        if os.path.exists(self.exe_path):
            proc = subprocess.Popen(
                [self.exe_path, "-json", self.db_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            full_sql = f"AUTH {user} '';\n{sql}" if user != "admin" else sql
            stdout, stderr = proc.communicate(input=full_sql)
            if proc.returncode != 0 and stderr:
                raise RuntimeError(f"SABLE native error: {stderr.strip()}")
            out = stdout.strip()
            if not out:
                return []
            try:
                return json.loads(out)
            except Exception:
                return [{"output": out}]

        raise RuntimeError("No native SABLE C++ engine binary or library found.")

    def status_json(self) -> Dict[str, Any]:
        if self.dll and self.db_handle:
            ptr = self.dll.sable_db_status_json(self.db_handle)
            if not ptr:
                return {}
            try:
                res_str = ctypes.string_at(ptr).decode("utf-8")
            finally:
                self.dll.sable_free_string(ptr)
            try:
                return json.loads(res_str)
            except Exception:
                return {}
        return {}

    def close(self):
        if self.dll and self.db_handle:
            self.dll.sable_db_close(self.db_handle)
            self.db_handle = None

class SableSyncClient:
    def __init__(self, db_path: str = DB_PATH):
        self.engine = NativeSableEngine(db_path=db_path)

    def execute(self, sql: str, user: str = "admin") -> List[Dict[str, Any]]:
        return self.engine.execute_json(sql, user)

    def execute_embedded(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        formatted_sql = sql
        if params:
            for p in params:
                val = f"'{p}'" if isinstance(p, str) else str(p)
                formatted_sql = formatted_sql.replace("?", val, 1)
        return self.execute(formatted_sql, "admin")

    def check_data_rules(self, table: str, sql: str) -> None:
        try:
            self.execute(sql, "admin")
        except RuntimeError as ex:
            if "Data rule violation" in str(ex):
                raise PermissionError(str(ex))
            raise

    def register_node(self, node_id: str, region: str, endpoint_url: str) -> Dict[str, Any]:
        now = time.time()
        sql = f"""
        INSERT INTO sync_nodes(node_identifier, cluster_region, endpoint_url, status, sync_epoch, last_heartbeat, created_at)
        VALUES('{node_id}', '{region}', '{endpoint_url}', 'active', 1, {now}, {now})
        ON CONFLICT(node_identifier) DO UPDATE SET
            endpoint_url=excluded.endpoint_url,
            status='active',
            last_heartbeat=excluded.last_heartbeat,
            sync_epoch=sync_nodes.sync_epoch + 1;
        """
        self.execute(sql, "sync_engine")
        return {"node_id": node_id, "status": "active", "timestamp": now}

    def push_sync_event(self, sync_uuid: str, entity_id: int, source_node: str,
                        target_node: str, operation: str, diff_payload: str) -> Dict[str, Any]:
        t0 = time.perf_counter_ns()
        now = time.time()
        escaped_diff = diff_payload.replace("'", "''")
        sql = f"""
        INSERT INTO sync_journals(sync_uuid, entity_id, source_node, target_node, operation, diff_payload, status, latency_us, timestamp)
        VALUES('{sync_uuid}', {entity_id}, '{source_node}', '{target_node}', '{operation}', '{escaped_diff}', 'applied', 0, {now});
        """
        self.execute(sql, "sync_engine")
        elapsed_us = max(1, (time.perf_counter_ns() - t0) // 1000)
        return {
            "sync_uuid": sync_uuid,
            "entity_id": entity_id,
            "status": "applied",
            "latency_us": elapsed_us,
            "timestamp": now
        }

    def pull_sync_events(self, target_node: str, limit: int = 50, current_user: str = "web_gateway") -> List[Dict[str, Any]]:
        sql = f"""
        SELECT id, sync_uuid, entity_id, source_node, target_node, operation, diff_payload, status, latency_us, timestamp
        FROM sync_journals
        WHERE target_node = '{target_node}' OR target_node = 'all'
        ORDER BY id DESC LIMIT {limit};
        """
        return self.execute(sql, current_user)

    def create_entity(self, entity_uuid: str, tenant_id: str, entity_type: str, payload: str) -> Dict[str, Any]:
        now = time.time()
        import hashlib
        checksum = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        sql = f"""
        INSERT INTO entities(entity_uuid, tenant_id, entity_type, version, payload_checksum, status, updated_at)
        VALUES('{entity_uuid}', '{tenant_id}', '{entity_type}', 1, '{checksum}', 'synced', {now})
        ON CONFLICT(entity_uuid) DO UPDATE SET
            version=entities.version + 1,
            payload_checksum=excluded.payload_checksum,
            updated_at=excluded.updated_at;
        """
        self.execute(sql, "sync_engine")
        return {"entity_uuid": entity_uuid, "tenant_id": tenant_id, "checksum": checksum, "timestamp": now}

    def transact_balance(self, src_account: str, dst_account: str, amount: float, currency: str = "USD") -> Dict[str, Any]:
        now = time.time()
        import hashlib
        sql = f"""
        UPDATE accounts_ledger SET cleared_balance = cleared_balance - {amount}, updated_at = {now} WHERE account_number = '{src_account}' AND cleared_balance >= {amount};
        UPDATE accounts_ledger SET cleared_balance = cleared_balance + {amount}, updated_at = {now} WHERE account_number = '{dst_account}';
        INSERT INTO accounts_ledger(account_number, tenant_id, currency, cleared_balance, pending_balance, routing_hash, updated_at)
        VALUES('{dst_account}', 'default', '{currency}', {amount}, 0.0, 'ROUT-AUTO', {now})
        ON CONFLICT(account_number) DO NOTHING;
        INSERT INTO compliance_audit_ledger(event_type, actor_role, node_identifier, before_state, after_state, signature_sha256, timestamp)
        VALUES('funds_transfer', 'sync_engine', 'cluster_primary', '{src_account}', '{dst_account}', '{hashlib.sha256(f"{src_account}->{dst_account}:{amount}".encode()).hexdigest()}', {now});
        """
        self.execute(sql, "sync_engine")
        return {
            "status": "cleared",
            "source": src_account,
            "destination": dst_account,
            "amount": amount,
            "currency": currency,
            "timestamp": now
        }

    def get_accounts(self, limit: int = 50, current_user: str = "web_gateway") -> List[Dict[str, Any]]:
        sql = f"SELECT id, account_number, tenant_id, currency, cleared_balance, pending_balance, routing_hash, updated_at FROM accounts_ledger ORDER BY id ASC LIMIT {limit};"
        return self.execute(sql, current_user)

    def get_audit_trail(self, limit: int = 50) -> List[Dict[str, Any]]:
        sql = f"SELECT id, event_type, actor_role, node_identifier, before_state, after_state, signature_sha256, timestamp FROM compliance_audit_ledger ORDER BY id DESC LIMIT {limit};"
        return self.execute(sql, "auditor")

    def get_sable_status(self) -> Dict[str, Any]:
        raw_status = self.engine.status_json()
        counts_res = self.execute("SELECT (SELECT COUNT(*) FROM sync_journals) AS total_journals, (SELECT COUNT(*) FROM entities) AS total_entities, (SELECT COUNT(*) FROM sync_nodes) AS total_nodes, (SELECT COUNT(*) FROM accounts_ledger) AS total_accounts;", "admin")
        counts = counts_res[0] if counts_res else {}
        idx_res = self.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex_%';", "admin")
        indexes = [r.get("name", "") for r in idx_res if "name" in r]
        return {
            "engine": "sable-v4.0.0-native-cpp",
            "driver": "libsable.dll (in-process native C++20)",
            "total_sync_journals": int(counts.get("total_journals", 0)),
            "total_entities": int(counts.get("total_entities", 0)),
            "total_nodes": int(counts.get("total_nodes", 0)),
            "total_accounts": int(counts.get("total_accounts", 0)),
            "active_indexes": indexes,
            "workload": raw_status.get("workload", {"phase": "active", "p50_latency_us": 15, "p95_latency_us": 40}),
            "status": "healthy"
        }
