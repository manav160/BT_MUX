# server/src/demo_runner.py

import asyncio
import time
import sys
from typing import List
from .core.scheduler import TDMScheduler
from .core.session_manager import SessionManager
from .demo_config import T_SLICE_MS, DEGRADED_TIMEOUT_MS, MAX_LOOP_WAIT_SEC_PER_CHUNK, LOOP_WAIT_STEP

async def execute_tdm_loop(
    transport,
    session_mgr: SessionManager,
    node_ids: List[str],
    total_expected_chunks: int
) -> dict:
    """Starts the scheduler, monitors progress, and returns execution metrics."""
    scheduler = TDMScheduler(
        transport=transport,
        session_manager=session_mgr,
        t_slice_ms=T_SLICE_MS,
        degraded_timeout_ms=DEGRADED_TIMEOUT_MS,
    )
    
    for nid in node_ids:
        scheduler.register_node(nid)
        session_mgr.get_or_create_socket(nid)

    print(f"\n[+] Starting TDM Scheduler loop across {len(node_ids)} physical nodes...")
    print(f"[!] NOTE: T_slice={T_SLICE_MS}ms, Degraded Timeout={DEGRADED_TIMEOUT_MS}ms\n")

    t_start = time.perf_counter()
    await scheduler.start()
    
    elapsed = 0.0
    CAP_TIME = max((MAX_LOOP_WAIT_SEC_PER_CHUNK*total_expected_chunks), 120)
    while elapsed < CAP_TIME:
        total_remaining = sum(session_mgr.get_buffer(nid).queue_depth for nid in node_ids)
        total_in_flight = sum(session_mgr.get_buffer(nid).in_flight_count for nid in node_ids)
        completed_so_far = sum(len(session_mgr.dedup_cache.get_completed_chunk_ids(nid)) for nid in node_ids)
        
        sys.stdout.write(
            f"\r    [TDM Loop: {elapsed:5.1f}s] Active: {len(scheduler.active_slots)} | "
            f"Degraded: {len(scheduler.degraded_nodes)} | Completed: {completed_so_far}/{total_expected_chunks} | Queued: {total_remaining}   "
        )
        sys.stdout.flush()

        if total_remaining == 0 and total_in_flight == 0 and completed_so_far == total_expected_chunks:
            break

        await asyncio.sleep(LOOP_WAIT_STEP)
        elapsed += LOOP_WAIT_STEP

    await scheduler.stop()
    t_end = time.perf_counter()
    
    return {
        "run_time_sec": t_end - t_start,
        "completed_chunks": sum(len(session_mgr.dedup_cache.get_completed_chunk_ids(nid)) for nid in node_ids)
    }