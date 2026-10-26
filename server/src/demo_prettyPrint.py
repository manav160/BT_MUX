"""BT-Mux Pretty Print Module.
Handles all console output formatting for the live demonstration CLI.
Separates UI/Display logic from core execution and telemetry logic.
"""
import sys
from typing import List, Dict, Any

def print_header(target_nodes: int, n_max_slots: int, t_slice_ms: int, chunks_per_node: int) -> None:
    """Prints the initial demonstration banner and configuration summary."""
    print("=" * 72)
    print("      BT-MUX USER-SPACE BLE VIRTUAL SOCKET MULTIPLEXER ")
    print("      PHYSICAL BLE TESTBED VALIDATION & DEMO (OPTION A)")
    print("=" * 72)
    print(f"[*] Target Logical Nodes (N >= 10): {target_nodes}")
    print(f"[*] Physical GATT Slots (N_max):    {n_max_slots}")
    print(f"[*] Time-Division Window (T_slice): {t_slice_ms} ms")
    print(f"[*] Payload Chunks Per Node:        {chunks_per_node}")
    print("-" * 72)

def print_scanning_start() -> None:
    """Prints scanning initialization messages."""
    print("[+] Scanning for physical BLE peripherals in radio range...")
    print("[!] Waiting 5 seconds to ensure Android devices are detected...")

def print_discovered_devices(devices: List[Dict[str, Any]]) -> None:
    """Prints the list of discovered BLE devices for user selection."""
    print(f"\n[+] Found {len(devices)} target demo devices.")
    print("Please select the devices you want to connect to:")
    for i, dev in enumerate(devices, 1):
        print(f"  [{i}] {dev['name']} | MAC: {dev['device_id']} | RSSI: {dev['rssi']} dBm")

def print_selected_devices(devices: List[Dict[str, Any]]) -> None:
    """Prints the final list of devices selected for the session."""
    print(f"\n[+] Selected {len(devices)} devices for this session:")
    for dev in devices:
        print(f"    -> {dev['name']} | MAC: {dev['device_id']}")

def print_payload_config() -> None:
    """Prints the payload configuration section header."""
    print("\n" + "-" * 72)
    print("PAYLOAD CONFIGURATION")
    print("-" * 72)

def print_enqueue_success(total_chunks: int) -> None:
    """Prints confirmation that chunks have been enqueued."""
    print(f"[+] Successfully enqueued {total_chunks} chunks into user-space RAM FIFOs")
    print("-" * 72)

def print_loop_status(elapsed: float, active: int, degraded: int, completed: int, total: int, queued: int) -> None:
    """Prints the live TDM loop status in-place using carriage return."""
    sys.stdout.write(
        f"\r    [TDM Loop: {elapsed:5.1f}s] Active: {active} | "
        f"Degraded: {degraded} | Completed: {completed}/{total} | Queued: {queued}   "
    )
    sys.stdout.flush()

def print_final_results(
    total_expected: int,
    completed: int,
    run_time: float,
    pdr: float,
    pdr_target: float,
    ram_mb: float,
    ram_limit: float,
    handoff_ms: float,
    handoff_target: float,
    all_sockets_open: bool,
) -> None:
    """Prints the final benchmark telemetry results in a formatted table."""
    pdr_status = "PASS" if pdr >= pdr_target else "FAIL"
    ram_status = "PASS" if ram_mb < ram_limit else "FAIL"
    handoff_status = "PASS" if handoff_ms <= handoff_target else "FAIL"
    socket_status = "ALL OPEN (REQ-5 PASS)" if all_sockets_open else "FAIL"

    print("\n" + "=" * 72)
    print("                    PHYSICAL BENCHMARK TELEMETRY RESULTS")
    print("=" * 72)
    print(f"Total Chunks Sent:              {total_expected}")
    print(f"Total Chunks Acknowledged:      {completed} (via Option A GATT Write-Response)")
    print(f"Packet Delivery Ratio (PDR):    {pdr * 100:.2f}%  (Target: >= {pdr_target * 100:.1f}%) -> {pdr_status}")
    print(f"Host Memory Footprint:          {ram_mb:.2f} MB (Ceiling: < {ram_limit} MB) -> {ram_status}")
    print(f"Mean Handoff Latency:           {handoff_ms:.2f} ms (Target: <= {handoff_target} ms) -> {handoff_status}")
    print(f"Logical Sockets Preserved:      {socket_status}")
    print(f"Total Benchmark Run Time:       {run_time:.2f} seconds")
    print("-" * 72)