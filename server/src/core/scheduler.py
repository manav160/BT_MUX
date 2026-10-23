"""BT-Mux Time-Division Multiplexing (TDM) Scheduler.

Authoritative source: BT-Mux_Agent_Instructions.md & SRS REQ-1, REQ-2, REQ-3, REQ-4.
Coordinates batch rotation of physical GATT slots across N >= 10 logical peripheral streams.
Decoupled strictly from BLE drivers via BLETransport interface (BT-Mux_Hardware_Abstraction_Layer.md).
"""
import asyncio
import logging
from typing import Dict, List, Optional, Set

from ..config.constants import (
    DEGRADED_TIMEOUT_MS,
    DEFAULT_T_SLICE_MS,
    T_SLICE_MAX_MS,
    T_SLICE_MIN_MS,
    SocketState,
)
from .session_manager import SessionManager
from .transport import BLETransport

logger = logging.getLogger("btmux.core.scheduler")


class TDMScheduler:
    """TDM Scheduler rotating N_max physical radio slots across N logical streams.
    
    Responsibilities:
    - Queries host controller cap N_max dynamically via transport (REQ-1).
    - Rotates peripheral batches in configurable T_slice windows (REQ-2).
    - Gracefully disconnects physical GATT slots prior to connecting the next batch (REQ-3).
    - Flags peripherals exceeding 1000ms connect timeout as DEGRADED_RETRY and bypasses slot (REQ-4).
    - Supports dynamic adjustment of T_slice during active sessions (REQ-11).
    """

    def __init__(
        self,
        transport: BLETransport,
        session_manager: SessionManager,
        t_slice_ms: int = DEFAULT_T_SLICE_MS,
        degraded_timeout_ms: int = DEGRADED_TIMEOUT_MS,
    ) -> None:
        self.transport: BLETransport = transport
        self.session_manager: SessionManager = session_manager

        # Dynamic detection of physical connection cap N_max from HAL adapter (SRS REQ-1)
        # Avoids hardcoding 4 so platform ports (e.g. Raspberry Pi) adapt to reported chipset caps.
        self.n_max: int = self.transport.get_max_connections()

        # Duty-cycling time window duration (SRS REQ-2)
        self.t_slice_ms: int = min(max(t_slice_ms, T_SLICE_MIN_MS), T_SLICE_MAX_MS)
        self.degraded_timeout_ms: int = degraded_timeout_ms

        # Round-robin queue of device IDs
        self.node_queue: List[str] = []

        # Currently occupied physical radio slots (bounded by self.n_max)
        self.active_slots: Set[str] = set()

        # Detailed physical slot mapping for Slot 1..N_max (Section 10)
        self.physical_slots: List[Optional[Dict[str, Any]]] = [None] * self.n_max

        # Nodes flagged as degraded due to connection timeout or link failure (REQ-4)
        self.degraded_nodes: Set[str] = set()

        # Node status callbacks for UI / Telemetry updates
        self._status_callbacks: List[callable] = []

        # Scheduler lifecycle loop task
        self._running: bool = False
        self._loop_task: Optional[asyncio.Task] = None
        self._lock: asyncio.Lock = asyncio.Lock()

        # Register session manager transport callback if not already configured
        if not self.session_manager._transport_send_fn:
            self.session_manager.set_transport_send_callback(self.transport.send)

    def get_slot_assignments(self) -> List[Dict[str, Any]]:
        """Return explicit physical slot assignments matching Section 10."""
        slots = []
        for i in range(self.n_max):
            slot_info = self.physical_slots[i] if i < len(self.physical_slots) else None
            if slot_info and slot_info.get("device_id"):
                slots.append({
                    "slot_index": i,
                    "slot_number": i + 1,
                    "device_id": slot_info["device_id"],
                    "state": slot_info.get("state", "CONNECTED"),
                    "is_active": slot_info.get("state") in ("CONNECTED", "TRANSMITTING", "ACTIVE_PHYSICAL_LINK"),
                })
            else:
                slots.append({
                    "slot_index": i,
                    "slot_number": i + 1,
                    "device_id": None,
                    "state": "IDLE",
                    "is_active": False,
                })
        return slots

    def register_node(self, device_id: str) -> None:
        """Register a logical node into the scheduler's round-robin queue."""
        if device_id not in self.node_queue:
            self.node_queue.append(device_id)
            self.session_manager.get_or_create_socket(device_id)
            logger.info("TDMScheduler: Registered node '%s' (total_nodes=%d)", device_id, len(self.node_queue))

    def unregister_node(self, device_id: str) -> None:
        """Remove a logical node from the scheduling queue."""
        if device_id in self.node_queue:
            self.node_queue.remove(device_id)
        self.active_slots.discard(device_id)
        self.degraded_nodes.discard(device_id)

    def set_t_slice_ms(self, duration_ms: int) -> None:
        """Dynamically update T_slice duration during active test run (SRS REQ-11)."""
        duration_ms = min(max(duration_ms, T_SLICE_MIN_MS), T_SLICE_MAX_MS)
        logger.info("TDMScheduler: Updated T_slice from %d ms to %d ms (REQ-11)", self.t_slice_ms, duration_ms)
        self.t_slice_ms = duration_ms

    async def start(self) -> None:
        """Start the background TDM scheduling loop."""
        async with self._lock:
            if self._running:
                return
            self._running = True
            self._loop_task = asyncio.create_task(self._run_loop(), name="tdm_scheduler_loop")
            logger.info(
                "TDMScheduler started (N_max=%d, T_slice=%d ms, total_nodes=%d)",
                self.n_max,
                self.t_slice_ms,
                len(self.node_queue),
            )

    async def stop(self) -> None:
        """Stop the scheduling loop and gracefully disconnect all active slots."""
        async with self._lock:
            if not self._running:
                return
            self._running = False
            if self._loop_task and not self._loop_task.done():
                self._loop_task.cancel()
                try:
                    await self._loop_task
                except asyncio.CancelledError:
                    pass

            # Gracefully tear down all active physical radio links
            active_devs = list(self.active_slots)
            for dev_id in active_devs:
                await self._disconnect_node_graceful(dev_id)
            self.physical_slots = [None] * self.n_max

            logger.info("TDMScheduler stopped. All physical radio slots freed.")

    async def step_rotation(self) -> None:
        """Execute a single deterministic TDM batch rotation step.
        
        Lifecycle sequence:
        1. Select next batch of up to N_max nodes from round-robin queue (REQ-1).
        2. Attempt connection with 1000ms timeout; flag timed-out nodes as DEGRADED (REQ-4).
        3. Hold connected batch active for T_slice time window (REQ-2).
        4. Gracefully disconnect physical radio slots prior to next batch (REQ-3).
        5. Advance round-robin queue.
        """
        if not self.node_queue:
            return

        batch_size = min(self.n_max, len(self.node_queue))
        current_batch = self.node_queue[:batch_size]
        connected_nodes: List[str] = []

        # Populate initial connecting slots (Section 4 & 10)
        for i in range(self.n_max):
            if i < len(current_batch):
                self.physical_slots[i] = {"slot_index": i, "device_id": current_batch[i], "state": "CONNECTING"}
            else:
                self.physical_slots[i] = None

        # 1. Establish physical links for current batch concurrently with timeout (SRS REQ-1, REQ-4)
        connect_results = await asyncio.gather(
            *(self._connect_node_with_timeout(dev_id) for dev_id in current_batch)
        )
        for i, (dev_id, connected) in enumerate(zip(current_batch, connect_results)):
            if connected:
                connected_nodes.append(dev_id)
                self.active_slots.add(dev_id)
                buf = self.session_manager.get_buffer(dev_id)
                has_work = buf and (buf.queue_depth > 0 or len(buf._in_flight) > 0)
                state = "TRANSMITTING" if has_work else "CONNECTED"
                self.physical_slots[i] = {"slot_index": i, "device_id": dev_id, "state": state}
            else:
                self.physical_slots[i] = {"slot_index": i, "device_id": dev_id, "state": "FAILED"}

        # 2. Run active transmission window for T_slice (SRS REQ-2)
        # Flush pump in SessionManager automatically drains RAM buffers during this window (REQ-8)
        slice_seconds = self.t_slice_ms / 1000.0
        await asyncio.sleep(slice_seconds)

        # 3. Gracefully tear down physical connections concurrently to free slots before next batch (SRS REQ-3)
        # Explicit disconnect prevents BLE controller buffer overrun and HCI error 0x09
        for i, dev_id in enumerate(connected_nodes):
            if i < len(self.physical_slots) and self.physical_slots[i]:
                self.physical_slots[i]["state"] = "DISCONNECTING"

        if connected_nodes:
            await asyncio.gather(*(self._disconnect_node_graceful(dev_id) for dev_id in connected_nodes))

        # Clear physical slots prior to next rotation batch (Return to IDLE)
        for i in range(self.n_max):
            self.physical_slots[i] = None

        # 4. Advance round-robin queue: rotate evaluated batch to the back
        self.node_queue = self.node_queue[batch_size:] + current_batch

    async def _connect_node_with_timeout(self, device_id: str) -> bool:
        """Connect to peripheral with strict DEGRADED timeout (SRS REQ-4)."""
        sock = self.session_manager.get_or_create_socket(device_id)
        sock.set_physical_state(SocketState.CONNECTING)

        timeout_sec = self.degraded_timeout_ms / 1000.0
        try:
            # Enforce 1000ms timeout threshold per SRS REQ-4
            await asyncio.wait_for(self.transport.connect(device_id), timeout=timeout_sec)
            self.degraded_nodes.discard(device_id)
            # Notify session manager to launch automated flushing pump (REQ-8)
            await self.session_manager.on_physical_link_connected(device_id)
            return True
        except (asyncio.TimeoutError, Exception) as exc:
            # Node failed to connect within 1000ms: flag as DEGRADED and bypass slot (REQ-4)
            logger.warning(
                "TDMScheduler: Node '%s' failed link establishment within %d ms -> DEGRADED (bypass slot): %s",
                device_id,
                self.degraded_timeout_ms,
                exc,
            )
            self.degraded_nodes.add(device_id)
            await self.session_manager.on_physical_link_degraded(device_id)
            return False

    async def _disconnect_node_graceful(self, device_id: str) -> None:
        """Gracefully release physical GATT connection and notify session manager (SRS REQ-3)."""
        try:
            await self.session_manager.on_physical_link_paused(device_id)
            await self.transport.disconnect(device_id)
        except Exception as exc:
            logger.warning("Error disconnecting node '%s': %s", device_id, exc)
        finally:
            self.active_slots.discard(device_id)

    async def _run_loop(self) -> None:
        """Internal continuous rotation loop."""
        try:
            while self._running:
                await self.step_rotation()
                # Brief cooperative yield between rotation handoffs
                await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception("Unexpected exception in TDMScheduler loop: %s", exc)
