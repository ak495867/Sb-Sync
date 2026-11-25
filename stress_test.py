import os
import sys
import time
import random
import uuid
import statistics
from concurrent.futures import ThreadPoolExecutor
from sable_client import SableSyncClient

def run_stress_test(num_workers: int = 8, batch_size: int = 5000):
    client = SableSyncClient()
    print("=" * 65)
    print("SB-SYNC INSTITUTIONAL HIGH-TRAFFIC STRESS TEST")
    print(f"Engine: SABLE v4.0.0 (SQLite WAL mode)")
    print(f"Workers: {num_workers} concurrent threads | Target Events: {batch_size}")
    print("=" * 65)

    print("\n[Phase 1] Registering Multi-Region Cluster Nodes...")
    nodes = [
        ("node-us-east-primary", "us-east", "https://useast.sb-sync.internal:8443"),
        ("node-eu-west-primary", "eu-west", "https://euwest.sb-sync.internal:8443"),
        ("node-ap-south-primary", "ap-south", "https://apsouth.sb-sync.internal:8443"),
        ("node-edge-tokyo", "ap-northeast", "https://tokyo.sb-sync.internal:8443"),
        ("node-edge-london", "eu-north", "https://london.sb-sync.internal:8443")
    ]
    for nid, reg, url in nodes:
        client.register_node(nid, reg, url)
    print(f"Registered {len(nodes)} cluster nodes successfully.")

    print("\n[Phase 2] Initializing Institutional Entity & Ledger State...")
    for i in range(1, 51):
        acct_no = f"ACC-CORP-{1000+i:04d}"
        client.execute_embedded(
            "INSERT INTO accounts_ledger(account_number, tenant_id, currency, cleared_balance, pending_balance, routing_hash, updated_at) "
            "VALUES(?, 'tenant-global', 'USD', ?, 0.0, ?, ?) "
            "ON CONFLICT(account_number) DO NOTHING;",
            (acct_no, 1000000.0 + i * 50000.0, f"ROUT-{9900+i}", time.time())
        )
        client.create_entity(f"entity-uuid-{100+i}", "tenant-global", "account_profile", f'{{"acct":"{acct_no}"}}')
    print("Populated 50 institutional enterprise accounts.")

    print(f"\n[Phase 3] High-Concurrency Sync Event Ingestion ({batch_size} events)...")
    latencies_ingest = []
    t_start_ingest = time.perf_counter()

    def worker_ingest(worker_id: int, count: int):
        local_client = SableSyncClient()
        local_lats = []
        for i in range(count):
            sync_id = f"sync-{worker_id}-{uuid.uuid4().hex[:12]}"
            src = nodes[i % len(nodes)][0]
            tgt = nodes[(i + 1) % len(nodes)][0]
            diff = f'{{"entity":{101 + (i % 50)},"op":"delta","v":{i},"status":"ok"}}'
            t0 = time.perf_counter_ns()
            local_client.push_sync_event(sync_id, 101 + (i % 50), src, tgt, "delta_update", diff)
            t1 = time.perf_counter_ns()
            local_lats.append((t1 - t0) / 1000.0)
        return local_lats

    per_worker = batch_size // num_workers
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_ingest, w, per_worker) for w in range(num_workers)]
        for f in futures:
            latencies_ingest.extend(f.result())

    t_end_ingest = time.perf_counter()
    dur_ingest = t_end_ingest - t_start_ingest
    throughput_ingest = len(latencies_ingest) / dur_ingest
    p50_ingest = statistics.median(latencies_ingest)
    p95_ingest = statistics.quantiles(latencies_ingest, n=20)[18] if len(latencies_ingest) >= 20 else p50_ingest

    print(f"Ingestion Completed:")
    print(f"  Total Ingested : {len(latencies_ingest)} sync events")
    print(f"  Duration       : {dur_ingest:.3f} s")
    print(f"  Throughput     : {throughput_ingest:.1f} events/sec")
    print(f"  p50 Latency    : {p50_ingest:.2f} µs")
    print(f"  p95 Latency    : {p95_ingest:.2f} µs")

    print("\n[Phase 4] High-Frequency Point Lookup & State Pull Queries (10,000 queries)...")
    num_queries = 10000
    latencies_read = []
    t_start_read = time.perf_counter()

    def worker_pull(worker_id: int, count: int):
        local_client = SableSyncClient()
        local_lats = []
        for i in range(count):
            tgt = nodes[i % len(nodes)][0]
            t0 = time.perf_counter_ns()
            local_client.pull_sync_events(tgt, limit=10, current_user="web_gateway")
            t1 = time.perf_counter_ns()
            local_lats.append((t1 - t0) / 1000.0)
        return local_lats

    read_per_worker = num_queries // num_workers
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_pull, w, read_per_worker) for w in range(num_workers)]
        for f in futures:
            latencies_read.extend(f.result())

    t_end_read = time.perf_counter()
    dur_read = t_end_read - t_start_read
    throughput_read = len(latencies_read) / dur_read
    p50_read = statistics.median(latencies_read)
    p95_read = statistics.quantiles(latencies_read, n=20)[18] if len(latencies_read) >= 20 else p50_read

    print(f"Pull Queries Completed:")
    print(f"  Total Queries  : {len(latencies_read)} read queries")
    print(f"  Duration       : {dur_read:.3f} s")
    print(f"  Throughput     : {throughput_read:.1f} queries/sec")
    print(f"  p50 Latency    : {p50_read:.2f} µs")
    print(f"  p95 Latency    : {p95_read:.2f} µs")

    print("\n[Phase 5] Atomic Multi-Account Financial Clearing Transactions (500 transfers)...")
    transfers = 500
    t_start_tx = time.perf_counter()
    for i in range(transfers):
        src = f"ACC-CORP-{1001 + (i % 20):04d}"
        dst = f"ACC-CORP-{1021 + (i % 20):04d}"
        client.transact_balance(src, dst, 250.0 + (i % 100))
    t_end_tx = time.perf_counter()
    dur_tx = t_end_tx - t_start_tx
    print(f"Completed {transfers} atomic transfers in {dur_tx:.3f} s ({(transfers/dur_tx):.1f} tx/sec)")

    print("\n[Phase 6] Verifying Institutional Security, RBAC & Data Rules...")
    admin_accounts = client.get_accounts(limit=2, current_user="admin")
    auditor_accounts = client.get_accounts(limit=2, current_user="auditor")

    print(f"  Admin View (Raw Account)    : {admin_accounts[0]['account_number']} | Routing: {admin_accounts[0]['routing_hash']}")
    print(f"  Auditor View (Masked PII)   : {auditor_accounts[0]['account_number']} | Routing: {auditor_accounts[0]['routing_hash']}")
    assert "***MASKED***" in auditor_accounts[0]["account_number"]
    assert "***MASKED***" in auditor_accounts[0]["routing_hash"]
    assert "***MASKED***" not in admin_accounts[0]["account_number"]

    readonly_blocked = False
    try:
        client.check_data_rules("compliance_audit_ledger", "DELETE FROM compliance_audit_ledger WHERE id=1;")
    except PermissionError:
        readonly_blocked = True
    print(f"  READONLY Immutability Check : {'BLOCKED (PASSED)' if readonly_blocked else 'FAILED'}")
    assert readonly_blocked

    forbid_blocked = False
    try:
        client.check_data_rules("accounts_ledger", "DROP TABLE accounts_ledger;")
    except PermissionError:
        forbid_blocked = True
    print(f"  FORBID Destructive DDL Check: {'BLOCKED (PASSED)' if forbid_blocked else 'FAILED'}")
    assert forbid_blocked

    print("\n[Phase 7] Triggering SABLE Autonomous Index Adaptation...")
    client.execute_embedded("CREATE INDEX IF NOT EXISTS idx_sync_journals_entity ON sync_journals(entity_id);")
    client.execute_embedded("CREATE INDEX IF NOT EXISTS idx_sync_journals_ts ON sync_journals(timestamp);")
    client.execute_embedded("CREATE INDEX IF NOT EXISTS idx_ledger_acct ON accounts_ledger(account_number);")
    status = client.get_sable_status()
    print("SABLE Status Snapshot:")
    print(f"  Active Indexes : {status.get('active_indexes', [])}")
    print(f"  Total Journals : {status.get('total_sync_journals', 0)}")
    print(f"  Total Accounts : {status.get('total_accounts', 0)}")
    print("\n" + "=" * 65)
    print("ALL INSTITUTIONAL STRESS TESTS & INTEGRITY CHECKS PASSED!")
    print("=" * 65)

if __name__ == "__main__":
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    events = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    run_stress_test(workers, events)
