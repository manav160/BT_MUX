# BT-Mux 
---
# Physical BLE Live Demonstration Guide

This document provides the complete, step-by-step protocol for setting up the physical Android peripherals and executing the live BT-Mux Time-Division Multiplexing (TDM) demonstration for the viva evaluation.

---

### 1. Prerequisites

**Hardware:**
- Windows 10/11 Laptop with a functional internal or USB Bluetooth 4.2+ controller.
- Two or more Android smartphones (Android 12 and Android 16 tested).

**Software:**
- **Android Devices:** Nordic Semiconductor "nRF Connect" app installed from the Google Play Store.
- **Windows Host:** Python 3.10+ environment with all project dependencies installed (specifically `bleak`, `pydantic-settings`, `psutil`).

---

### 2. Android Device Configuration (nRF Connect)

Perform the following steps on **every** Android device participating in the demo. 

#### Step 2.1: Configure the Advertiser
The Advertiser makes the phone visible to the Windows Bluetooth scanner. It is critical to include the custom Service UUID in the advertising packet so the host can discover the custom GATT server.

1. Open **nRF Connect** and navigate to the **Advertiser** tab (bottom right).
2. Tap **Add new advertisement**.
3. Set the **Display name** to a unique identifier (e.g., `BTv2_P1`, `BTv2_P2`).
4. Under **Advertising data**, ensure **Flags** and **Complete Local Name** are present.
5. Tap **ADD RECORD** and select **Service UUID**.
6. Enter the exact Service UUID: `6e400001-b5a3-f393-e0a9-e50e24dcca9e`
7. Under **Options**, ensure **Connectable** is checked.
8. Tap **OK** to save the advertisement.
9. Tap the **Play/Start** button to begin broadcasting.

#### Step 2.2: Configure the GATT Server
The GATT Server acts as the actual data pipe. The Python script will write data to the characteristic configured here.

1. Navigate to the **GATT SERVER** tab (middle tab).
2. Tap the **+** (plus) icon to create a new server configuration.
3. **Add a Service**:
   - Tap **Add service**.
   - Tap **Add UUID** and enter: `6e400001-b5a3-f393-e0a9-e50e24dcca9e`
   - Tap **Add**.
4. **Add a Characteristic**:
   - Tap on the newly created service to expand it.
   - Tap **Add characteristic**.
   - Tap **Add UUID** and enter: `6e400002-b5a3-f393-e0a9-e50e24dcca9e`
   - **Properties**: Check the box for **Write**. (Do not check "Write without response").
   - **Permissions**: Scroll down and check the box for **Write**. (This is critical; without this, the OS will reject the write request with a "NO PERMIT" error).
   - Tap **Add**.
5. Tap the **Play/Start** button at the top right of the screen to start the GATT Server.

**Crucial Rule:** The nRF Connect app must remain open and in the **foreground** on all Android devices during the entire demonstration. Android OS aggressively suspends background BLE services.

---

### 3. Execution on Windows Host

1. Open the Command Prompt (cmd) or PowerShell on your Windows laptop.
2. Navigate to the root directory of your project:
   ```cmd
   cd E:\College materials\Sem5\BTP_v2\BT_MUX
   ```
3. Set the Python path and execute the production demo script:
   ```cmd
   set PYTHONPATH=server
   python server/src/prod_demo.py
   ```

#### Interactive Selection
The script will scan for 5 seconds and filter out ambient BLE noise (earbuds, laptops). It will present a numbered list of devices starting with `BTv2_`.

When prompted:
```text
Enter the numbers of the devices to connect to (comma or space separated): 
```
Type the numbers corresponding to your Android phones (e.g., `1,2`) and press Enter.

---

### 4. Expected Output and Verification

#### Windows Terminal Output
A successful run will show the following sequence:
1. Initialization of the Hardware Abstraction Layer (WinRT).
2. Discovery of the `BTv2_` devices.
3. Connection establishment (this may take 2 to 4 seconds per batch due to physical GAP handshake latencies).
4. The TDM Loop progress bar updating in real-time:
   ```text
   [TDM Loop:   6.0s] Active Slots: 2 | Degraded: 0 | Completed: 10/10 | Queued: 0
   ```
5. The final **PHYSICAL BENCHMARK TELEMETRY RESULTS** table showing:
   - Packet Delivery Ratio (PDR): 100.00% (PASS)
   - Host Memory Footprint: < 150 MB (PASS)
   - Mean Handoff Latency: <= 300 ms (PASS)

#### Android Device Verification (nRF Connect)
To prove to the evaluation panel that the data actually traversed the physical radio:
1. On the Android phone, stay on the **GATT SERVER** tab.
2. Look at the bottom of the screen or tap the small log icon to open the **Server log**.
3. You will see incoming entries labeled **WRITE REQUEST** for the characteristic `6e400002...`.
4. The value will be displayed in hexadecimal (e.g., `56-49-56-41-5F-31-3A-45-45-5F-30`). 
5. This hex translates to the ASCII string `VIVA_1:EE_0`, proving the payload was successfully written by the Windows host and acknowledged by the Android peripheral.

---

### 5. Troubleshooting

**Issue: "ERROR: No devices starting with 'BTv2_' found"**
- **Cause:** The Advertiser is not running, or the Service UUID was not added to the advertising packet.
- **Fix:** Verify Step 2.1. Ensure the Advertiser is started and the Service UUID record is present.

**Issue: Devices connect but immediately go to "DEGRADED" state**
- **Cause:** The Windows Bluetooth stack is timing out during the GATT handshake, or the GATT Server on the phone is not running.
- **Fix:** Verify Step 2.2. Ensure the GATT Server Play button is active. Close and reopen the nRF Connect app on the phone to clear any stale Bluetooth states, then restart both the Advertiser and GATT Server.

**Issue: Script completes with 100% PDR, but no data appears in the nRF Connect logs**
- **Cause:** The Characteristic "Write" permission is missing, or the script is falling back to a silent failure mode.
- **Fix:** Edit the characteristic in the GATT Server and ensure the **Write** box is checked under the **Permissions** section, not just the Properties section.

---

### 6. Viva Defense Talking Points

If the evaluation panel asks questions during the live demo, use the following technical justifications:

- **On the Hardware Abstraction Layer (HAL):** 
  "The core engine, including the TDM Scheduler and Session Manager, interacts strictly with the `BLETransport` abstract interface. It has no knowledge of the underlying `bleak` library or WinRT API. This allows us to swap the physical radio for a simulated transport for unit testing without changing a single line of core logic."

- **On the ACK Mechanism (Option A):** 
  "Because our physical peripherals are running standard GATT servers, we rely on the BLE Link Layer's Write-with-Response mechanism. When the `write_gatt_char` coroutine completes without throwing an exception, the Windows WinRT stack has already received the hardware-level acknowledgment from the phone. We intercept this success to satisfy the Session Manager's state machine, ensuring our PDR metrics reflect actual over-the-air delivery."

- **On the T_slice Duration:** 
  "The physical demo uses a `T_slice` of 1500ms, whereas the simulated demo uses 100ms. Physical BLE GAP handshakes incur a fixed latency penalty of 100 to 300 milliseconds per reconnection. A 1500ms window ensures the connection stabilizes and the RAM buffer flush pump has adequate time to transmit the chunks before the TDM slot expires."

- **On Connection Limits:** 
  "Standard BLE controllers are limited to 4 to 7 concurrent physical connections. BT-Mux bypasses this by maintaining N >= 10 logical virtual sockets in user-space RAM. The TDM scheduler rotates the physical radio slots across batches of nodes. When a physical link drops, the logical socket remains open, and the data is safely buffered until the next TDM cycle."
---
### Service: `6e400001-b5a3-f393-e0a9-e50e24dcca9e`

### RX: `6e400002-b5a3-f393-e0a9-e50e24dcca9e`

### TX: `6e400003-b5a3-f393-e0a9-e50e24dcca9e`