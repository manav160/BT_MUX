# server/src/demo_scanner.py

import asyncio
from typing import List, Dict, Any
from .hal.base import BLETransport
from .demo_config import TARGET_DEVICE_PREFIX, SCAN_WAIT_TIME_SEC

async def scan_and_select_devices(transport: BLETransport) -> List[Dict[str, Any]]:
    """Scans for BLE devices, filters by prefix, and prompts user for selection."""
    print(f"[!] Waiting {SCAN_WAIT_TIME_SEC} seconds to ensure Android devices are detected...")
    await asyncio.sleep(SCAN_WAIT_TIME_SEC)
    
    discovered_devices = await transport.scan_for_devices()
    filtered_devices = [
        dev for dev in discovered_devices 
        if dev.get("name") and dev["name"].startswith(TARGET_DEVICE_PREFIX)
    ]
    
    if not filtered_devices:
        print(f"[-] ERROR: No devices starting with '{TARGET_DEVICE_PREFIX}' found.")
        return []

    print(f"\n[+] Found {len(filtered_devices)} target demo devices.")
    print("Please select the devices you want to connect to:")
    for i, dev in enumerate(filtered_devices, 1):
        print(f"  [{i}] {dev['name']} | MAC: {dev['device_id']} | RSSI: {dev['rssi']} dBm")

    loop = asyncio.get_running_loop()
    while True:
        user_input = await loop.run_in_executor(
            None, input, "\nEnter the numbers of the devices (comma or space separated): "
        )
        if not user_input.strip():
            print("Invalid input. Please enter at least one number.")
            continue
        try:
            indices = [int(x) for x in user_input.replace(',', ' ').split()]
            if any(i < 1 or i > len(filtered_devices) for i in indices):
                print(f"Error: Indices must be between 1 and {len(filtered_devices)}.")
                continue
            selected_indices = list(dict.fromkeys(indices))
            return [filtered_devices[i - 1] for i in selected_indices]
        except ValueError:
            print("Invalid input. Please enter numbers only.")