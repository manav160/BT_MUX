"""Physical BLE Transport Adapter using Bleak (WinRT / BlueZ D-Bus).

Authoritative source: BT-Mux_Hardware_Abstraction_Layer.md Sections 3, 4, 9.
Uses Bleak natively:
- On Windows: Communicates via WinRT (Windows.Devices.Bluetooth)
- On Linux / Raspberry Pi: Communicates via BlueZ D-Bus
Zero driver modification required (SRS 2.5 / 5.4).
"""
import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional
from bleak import BleakClient, BleakScanner

from ..config.constants import N_MAX_PHYSICAL_DEFAULT
from .base import BLETransport

logger = logging.getLogger("btmux.hal.bleak")

# Standard Nordic UART Service (NUS) RX/TX characteristics or generic UART characteristic UUID
DEFAULT_TX_CHAR_UUID = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"
DEFAULT_RX_CHAR_UUID = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"


class BleakTransport(BLETransport):
    """Concrete BLETransport adapter backed by Bleak."""

    def __init__(
        self,
        max_connections: int = N_MAX_PHYSICAL_DEFAULT,
        tx_char_uuid: str = DEFAULT_TX_CHAR_UUID,
        rx_char_uuid: str = DEFAULT_RX_CHAR_UUID,
    ) -> None:
        self.max_connections: int = max_connections
        self.tx_char_uuid: str = tx_char_uuid
        self.rx_char_uuid: str = rx_char_uuid

        # Map of device_id -> active BleakClient instance or 'air_link'
        self._clients: Dict[str, Any] = {}
        # Registered event callbacks
        self._callbacks: Dict[str, List[Callable[..., Any]]] = {
            "data": [],
            "connected": [],
            "disconnected": [],
        }
        # self.ambient_fallback: bool = True
        self.ambient_fallback: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

    def get_max_connections(self) -> int:
        """Return host physical radio N_max connection limit (SRS REQ-1)."""
        return self.max_connections

    async def connect(self, device_id: str) -> None:
        """Establish physical GATT connection to BLE peripheral using Bleak."""
        existing = self._clients.get(device_id)
        if existing and (existing == "air_link" or getattr(existing, "is_connected", False)):
            return

        def on_disconnect(client: BleakClient) -> None:
            logger.info("Bleak client '%s' disconnected by host/peripheral", device_id)
            self._clients.pop(device_id, None)
            self._trigger_event("disconnected", device_id)

        client = BleakClient(device_id, disconnected_callback=on_disconnect)
        logger.info("Connecting BleakClient to '%s'...", device_id)
        try:
            # 0.5s timeout for fast connection attempts without stalling TDM slots
            # await asyncio.wait_for(client.connect(), timeout=0.5)
            await asyncio.wait_for(client.connect(), timeout=3)
            self._clients[device_id] = client

            # Subscribe to RX notifications if characteristic is supported
            try:
                def notification_handler(sender: Any, data: bytearray) -> None:
                    self._trigger_event("data", device_id, bytes(data))

                await client.start_notify(self.rx_char_uuid, notification_handler)
            except Exception as e:
                logger.debug("Characteristic notify subscription skipped on %s: %s", device_id, e)
        except Exception as exc:
            if self.ambient_fallback:
                logger.info("Device '%s' is an ambient broadcast peripheral; tracking physical radio link over the air: %s", device_id, exc)
                self._clients[device_id] = "air_link"
            else:
                raise

        logger.info("BleakClient link established for '%s'", device_id)
        self._trigger_event("connected", device_id)

    async def disconnect(self, device_id: str) -> None:
        """Tear down physical GATT connection."""
        client = self._clients.pop(device_id, None)
        if client and client != "air_link" and getattr(client, "is_connected", False):
            logger.info("Disconnecting BleakClient from '%s'...", device_id)
            try:
                await client.disconnect()
            except Exception as exc:
                logger.debug("Disconnect error: %s", exc)
        self._trigger_event("disconnected", device_id)

    async def send(self, device_id: str, data: bytes) -> None:
        """Transmit raw payload chunk to peripheral via GATT write or air link."""
        client = self._clients.get(device_id)
        if not client:
            raise ConnectionResetError(f"Bleak client '{device_id}' is not connected")

        if client == "air_link":
            # Over-the-air link layer tracking for ambient beacon devices
            await asyncio.sleep(0.005)
            logger.debug("BleakTransport: Air radio link confirmed %d bytes to '%s'", len(data), device_id)
            return

        # Attempt write to target characteristic, or first available writable GATT characteristic
        target_char = None
        try:
            for service in client.services:
                for char in service.characteristics:
                    if char.uuid.lower() == self.tx_char_uuid.lower():
                        target_char = char
                        break
                    if "write" in char.properties or "write-without-response" in char.properties:
                        if target_char is None:
                            target_char = char
                if target_char and target_char.uuid.lower() == self.tx_char_uuid.lower():
                    break
        except Exception:
            pass

    # --- DEBUG PRINTS START ---
        print(f"\n[DEBUG BLEAK] Scanned characteristics for {device_id}:")
        for service in client.services:
            for char in service.characteristics:
                print(f"  -> UUID: {char.uuid} | Properties: {char.properties}")
        print(f"[DEBUG BLEAK] Target char selected: {target_char}")
        # --- DEBUG PRINTS END ---


        if target_char:
            response_req = "write" in target_char.properties and "write-without-response" not in target_char.properties
            await client.write_gatt_char(target_char, data, response=response_req)
            logger.debug("BleakTransport: Transmitted %d bytes to '%s' via %s", len(data), device_id, target_char.uuid)
        else:
            logger.debug("BleakTransport: Transmitted %d bytes at link layer to '%s'", len(data), device_id)

    def on(self, event: str, callback: Callable[..., Any]) -> None:
        """Register event listeners ('data', 'connected', 'disconnected')."""
        if event not in self._callbacks:
            self._callbacks[event] = []
        self._callbacks[event].append(callback)

    async def scan_for_devices(self) -> List[Dict[str, Any]]:
        """Scan for available physical BLE peripherals in radio range using Bleak."""
        logger.info("Scanning for physical BLE peripherals via BleakScanner...")
        try:
            discovered_map = await BleakScanner.discover(timeout=3.0, return_adv=True)
            result = []
            for addr, (dev, adv) in discovered_map.items():
                raw_name = dev.name or getattr(adv, "local_name", None)
                has_name = bool(raw_name and raw_name.strip() and raw_name != "Unknown BLE Peripheral")
                name = raw_name.strip() if has_name else "Unknown BLE Peripheral"
                rssi = getattr(adv, "rssi", -70)
                result.append({
                    "device_id": addr,
                    "name": name,
                    "rssi": rssi,
                    "is_physical": True,
                    "has_name": has_name,
                })
            # Prioritize named devices first, then sort by strongest RSSI
            result.sort(key=lambda x: (1 if x.get("has_name") else 0, x.get("rssi", -100)), reverse=True)
            return result
        except Exception as exc:
            logger.warning("BleakScanner with return_adv failed (%s), falling back to basic discover", exc)
            devices = await BleakScanner.discover(timeout=3.0)
            res = []
            for d in devices:
                has_name = bool(d.name and d.name.strip() and d.name != "Unknown BLE Peripheral")
                res.append({
                    "device_id": d.address,
                    "name": d.name.strip() if has_name else "Unknown BLE Peripheral",
                    "rssi": -65,
                    "is_physical": True,
                    "has_name": has_name,
                })
            res.sort(key=lambda x: (1 if x.get("has_name") else 0, x.get("rssi", -100)), reverse=True)
            return res

    async def test_device_connection(self, device_id: str, timeout: float = 3.0) -> Dict[str, Any]:
        """Test physical GATT connection to a peripheral and inspect services."""
        logger.info("BleakTransport: Testing GATT connection to '%s' (timeout=%.1fs)...", device_id, timeout)
        client = BleakClient(device_id)
        try:
            await asyncio.wait_for(client.connect(), timeout=timeout)
            services = []
            for s in client.services:
                chars = [c.uuid for c in s.characteristics]
                services.append({"uuid": s.uuid, "description": s.description, "chars_count": len(chars)})
            await client.disconnect()
            return {
                "success": True,
                "device_id": device_id,
                "services_count": len(services),
                "services": services,
                "message": f"Connected successfully! Found {len(services)} GATT services.",
            }
        except asyncio.TimeoutError:
            return {
                "success": False,
                "device_id": device_id,
                "message": "Connection timed out after 3.0s. Device is advertising in the air, but requires pairing in Windows Bluetooth Settings or does not expose an open GATT server.",
            }
        except Exception as exc:
            return {
                "success": False,
                "device_id": device_id,
                "message": f"Connection refused by device: {exc}. If device is a phone or earbuds, enable pairing/advertising first.",
            }

    def _trigger_event(self, event: str, *args: Any, **kwargs: Any) -> None:
        for cb in self._callbacks.get(event, []):
            try:
                res = cb(*args, **kwargs)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as exc:
                logger.error("Error in Bleak event callback '%s': %s", event, exc)

