"""In-Memory Simulated BLE Transport for Evaluation Without Hardware.

Authoritative source: BT-Mux_Hardware_Abstraction_Layer.md Section 5.
Simulates peripheral discovery, connection latency (100-300ms, SRS 2.5),
and reliable packet delivery for N >= 10 virtual nodes.
"""
import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Set

from ..config.constants import N_MAX_PHYSICAL_DEFAULT
from .base import BLETransport

logger = logging.getLogger("btmux.hal.simulated")


class SimulatedTransport(BLETransport):
    """In-memory BLETransport simulator conforming strictly to the HAL contract."""

    def __init__(
        self,
        max_connections: int = N_MAX_PHYSICAL_DEFAULT,
        handshake_delay_sec: float = 0.02,
        disconnect_delay_sec: float = 0.005,
        transmission_delay_sec: float = 0.005,
    ) -> None:
        self.max_connections: int = max_connections
        self.handshake_delay_sec: float = handshake_delay_sec
        self.disconnect_delay_sec: float = disconnect_delay_sec
        self.transmission_delay_sec: float = transmission_delay_sec

        # Active physical connections: device_id -> True
        self.connected_devices: Set[str] = set()
        # Simulated peripherals discoverable via scan
        self.simulated_devices: List[Dict[str, Any]] = [
            {"device_id": f"sim_node_{i:02d}", "name": f"Simulated Node {i:02d}", "rssi": -60 - (i % 20)}
            for i in range(12)
        ]
        # Transmitted payloads: list of (device_id, bytes)
        self.transmitted_history: List[tuple[str, bytes]] = []
        # Receiver acceptance tracking for duplicate detection (Section 6)
        # device_id -> set of accepted chunk keys
        self.receiver_accepted: Dict[str, Set[str]] = {}
        # Fault injection rate for lost packets or lost ACKs (Section 6 & 12, disabled by default)
        self.fault_injection_loss_rate: float = 0.0

        # Event callbacks: event_name -> list[callable]
        self._callbacks: Dict[str, List[Callable[..., Any]]] = {
            "data": [],
            "connected": [],
            "disconnected": [],
        }
        # Configurable fault injection for testing DEGRADED node handling (REQ-4)
        self.failing_devices: Set[str] = set()
        self._lock: asyncio.Lock = asyncio.Lock()

    def set_fault_injection(self, enabled: bool, loss_rate: float = 0.20) -> None:
        """Configure simulated packet/ACK loss fault injection (Section 6 & 12)."""
        self.fault_injection_loss_rate = loss_rate if enabled else 0.0

    def clear_receiver_state(self) -> None:
        """Reset simulated receivers accepted chunk cache."""
        self.receiver_accepted.clear()

    def get_max_connections(self) -> int:
        """Return host physical radio N_max connection limit (SRS REQ-1)."""
        return self.max_connections

    async def connect(self, device_id: str) -> None:
        """Simulate GATT link handshake with realistic connection latency (SRS 2.5)."""
        if device_id in self.failing_devices:
            # Inject link timeout / failure for DEGRADED node evaluation
            await asyncio.sleep(self.handshake_delay_sec)
            raise ConnectionError(f"Simulated link timeout/error connecting to '{device_id}'")

        await asyncio.sleep(self.handshake_delay_sec)
        async with self._lock:
            self.connected_devices.add(device_id)

        logger.debug("SimulatedTransport: Connected to '%s' (active=%d)", device_id, len(self.connected_devices))
        self._trigger_event("connected", device_id)

    async def disconnect(self, device_id: str) -> None:
        """Simulate graceful GATT teardown."""
        await asyncio.sleep(self.disconnect_delay_sec)
        async with self._lock:
            self.connected_devices.discard(device_id)

        logger.debug("SimulatedTransport: Disconnected '%s' (active=%d)", device_id, len(self.connected_devices))
        self._trigger_event("disconnected", device_id)

    async def send(self, device_id: str, data: bytes) -> None:
        """Simulate over-the-air chunk transmission to peripheral."""
        if device_id not in self.connected_devices:
            raise ConnectionResetError(f"Cannot send to disconnected device '{device_id}'")

        await asyncio.sleep(self.transmission_delay_sec)
        self.transmitted_history.append((device_id, data))
        logger.debug("SimulatedTransport: Sent %d bytes to '%s'", len(data), device_id)

    def on(self, event: str, callback: Callable[..., Any]) -> None:
        """Register event listeners ('data', 'connected', 'disconnected')."""
        if event not in self._callbacks:
            self._callbacks[event] = []
        self._callbacks[event].append(callback)

    async def scan_for_devices(self) -> List[Dict[str, Any]]:
        """Return simulated BLE peripheral list."""
        await asyncio.sleep(0.01)
        return list(self.simulated_devices)

    def inject_incoming_data(self, device_id: str, data: bytes) -> None:
        """Test helper to simulate incoming peripheral notification."""
        self._trigger_event("data", device_id, data)

    def _trigger_event(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Dispatch registered event callbacks."""
        for cb in self._callbacks.get(event, []):
            try:
                res = cb(*args, **kwargs)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as exc:
                logger.error("Error in event callback for '%s': %s", event, exc)
