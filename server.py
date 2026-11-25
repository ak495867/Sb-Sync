import os
import sys
import json
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from sable_client import SableSyncClient

PORT = int(os.environ.get("SB_SYNC_PORT", 8080))
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
client = SableSyncClient()

class SableSyncRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, status_code: int, data: Any):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-User-Role")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-User-Role")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            html_file = os.path.join(STATIC_DIR, "index.html")
            if os.path.exists(html_file):
                with open(html_file, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            self.send_json(404, {"error": "Dashboard not found"})
            return

        if path == "/api/v1/health":
            self.send_json(200, {"status": "ok", "service": "sb-sync", "engine": "sable-v4.0.0", "timestamp": time.time()})
            return

        if path == "/api/v1/nodes":
            rows = client.execute_embedded("SELECT id, node_identifier, cluster_region, endpoint_url, status, sync_epoch, last_heartbeat FROM sync_nodes ORDER BY last_heartbeat DESC LIMIT 50;")
            self.send_json(200, {"nodes": rows, "count": len(rows)})
            return

        if path == "/api/v1/sync/pull":
            target = query.get("target", ["all"])[0]
            limit = int(query.get("limit", [50])[0])
            user_role = self.headers.get("X-User-Role", query.get("user", ["web_gateway"])[0])
            events = client.pull_sync_events(target, limit, user_role)
            self.send_json(200, {"target": target, "events": events, "count": len(events)})
            return

        if path == "/api/v1/ledger/accounts":
            limit = int(query.get("limit", [50])[0])
            user_role = self.headers.get("X-User-Role", query.get("user", ["web_gateway"])[0])
            accounts = client.get_accounts(limit, user_role)
            self.send_json(200, {"user_role": user_role, "accounts": accounts, "count": len(accounts)})
            return

        if path == "/api/v1/compliance/audit":
            limit = int(query.get("limit", [50])[0])
            trail = client.get_audit_trail(limit)
            self.send_json(200, {"audit_trail": trail, "count": len(trail)})
            return

        if path == "/api/v1/sable/metrics":
            metrics = client.get_sable_status()
            rules = client.execute_embedded("SELECT rule_name, target_table, rule_type, expression FROM _sable_rules;")
            metrics["security_rules"] = rules
            self.send_json(200, metrics)
            return

        self.send_json(404, {"error": "Not Found", "path": path})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length) if length > 0 else b"{}"

        try:
            body = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        except Exception:
            body = {}

        if path == "/api/v1/nodes/register":
            node_id = body.get("node_id")
            region = body.get("region", "us-east")
            url = body.get("endpoint_url", f"http://{node_id}.internal")
            if not node_id:
                self.send_json(400, {"error": "Missing node_id"})
                return
            res = client.register_node(node_id, region, url)
            self.send_json(200, res)
            return

        if path == "/api/v1/sync/push":
            sync_uuid = body.get("sync_uuid", f"sync-{int(time.time()*1000)}")
            entity_id = int(body.get("entity_id", 1))
            source_node = body.get("source_node", "node-edge-1")
            target_node = body.get("target_node", "all")
            operation = body.get("operation", "upsert")
            diff_payload = body.get("diff_payload", "{}")
            if isinstance(diff_payload, dict):
                diff_payload = json.dumps(diff_payload)
            res = client.push_sync_event(sync_uuid, entity_id, source_node, target_node, operation, diff_payload)
            self.send_json(200, res)
            return

        if path == "/api/v1/entities":
            entity_uuid = body.get("entity_uuid")
            tenant_id = body.get("tenant_id", "default")
            entity_type = body.get("entity_type", "record")
            payload = body.get("payload", "{}")
            if not entity_uuid:
                self.send_json(400, {"error": "Missing entity_uuid"})
                return
            res = client.create_entity(entity_uuid, tenant_id, entity_type, payload)
            self.send_json(200, res)
            return

        if path == "/api/v1/ledger/transact":
            src = body.get("source_account")
            dst = body.get("destination_account")
            amount = float(body.get("amount", 0.0))
            currency = body.get("currency", "USD")
            if not src or not dst or amount <= 0:
                self.send_json(400, {"error": "Invalid source, destination, or amount"})
                return
            try:
                res = client.transact_balance(src, dst, amount, currency)
                self.send_json(200, res)
            except Exception as ex:
                self.send_json(400, {"error": str(ex)})
            return

        if path == "/api/v1/sable/adapt":
            try:
                conn = client._get_connection()
                cur = conn.cursor()
                cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_journals_entity ON sync_journals(entity_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_journals_ts ON sync_journals(timestamp);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ledger_acct ON accounts_ledger(account_number);")
                conn.commit()
                conn.close()
                self.send_json(200, {"status": "adapted", "indexes_created": ["idx_sync_journals_entity", "idx_sync_journals_ts", "idx_ledger_acct"]})
            except Exception as ex:
                self.send_json(500, {"error": str(ex)})
            return

        self.send_json(404, {"error": "Not Found", "path": path})

def run_server():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), SableSyncRequestHandler)
    print(f"SB-Sync Web Gateway running on http://127.0.0.1:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()

if __name__ == "__main__":
    run_server()
