"""BT-Mux Session Manager and Auto-Flushing Pump.

Authoritative source: BT-Mux_Agent_Instructions.md & SRS REQ-5, REQ-6, REQ-7, REQ-8.
Decoupled completely from physical BLE libraries per BT-Mux_Hardware_Abstraction_Layer.md.
"""
import asyncio
import inspect
import logging
from typing import Any, Callable, Coroutine, Dict, Optional

from ..config.constants import SocketState
from .buffer import StreamBuffer
from .chunk import Chunk
from .dedup_cache import DedupCache
from .virtual_socket import VirtualSocket

logger = logging.getLogger("btmux.core.session_manager")

# Type alias for decoupled HAL transmission callback: async def send_bytes(device_id: str, data: bytes) -> None
TransportSendCallback = Callable[[str, bytes], Coroutine[Any, Any, None]]


class SessionManager:
    """Coordinates virtual sockets, stream buffers, and auto-flushing transmission pumps.
    
    Responsibilities:
    - Maintains the mapping of logical streams (N >= 10) across active session nodes.
    - Isolates buffers in RAM to ensure physical disconnects are invisible to higher layers (REQ-7).
    - Automatically launches flushing pumps on GATT_CONNECTED to dispatch pending bytes (REQ-8).
    - Integrates with the 5-minute dedup cache (Section 4.1).
    """

    def __init__(
        self,
        session_id: str,
        transport_send_fn: Optional[TransportSendCallback] = None,
        dedup_cache: Optional[DedupCache] = None,
    ) -> None:
        self.session_id: str = session_id
        # Injected transport callback — completely decoupled from bleak / drivers (HAL rule)
        self._transport_send_fn: Optional[TransportSendCallback] = transport_send_fn
        # Shared dedup cache across all streams in this session
        self.dedup_cache: DedupCache = dedup_cache or DedupCache()
        # Persistent counter for total completed chunks (never decrements)
        self.total_completed_chunks: int = 0

        # Logical sockets mapped by device_id
        self.sockets: Dict[str, VirtualSocket] = {}
        # Dedicated per-stream buffers mapped by device_id (REQ-6)
        self.buffers: Dict[str, StreamBuffer] = {}

        # Active background flushing tasks per device_id
        self._flush_tasks: Dict[str, asyncio.Task] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    def get_or_create_socket(self, device_id: str) -> VirtualSocket:
        """Retrieve existing or initialize a new VirtualSocket and StreamBuffer for a device."""
        if device_id not in self.sockets:
            sock = VirtualSocket(device_id=device_id, session_id=self.session_id)
            buf = StreamBuffer(device_id=device_id, dedup_cache=self.dedup_cache)
            self.sockets[device_id] = sock
            self.buffers[device_id] = buf
            logger.info("SessionManager[%s]: Created stream for device '%s'", self.session_id, device_id)
        return self.sockets[device_id]

    def get_socket(self, device_id: str) -> Optional[VirtualSocket]:
        """Look up existing virtual socket by device ID."""
        return self.sockets.get(device_id)

    def get_buffer(self, device_id: str) -> Optional[StreamBuffer]:
        """Look up dedicated stream buffer by device ID."""
        return self.buffers.get(device_id)

    def set_transport_send_callback(self, fn: TransportSendCallback) -> None:
        """Inject or update the hardware-agnostic transmission callback."""
        self._transport_send_fn = fn

    async def on_physical_link_connected(self, device_id: str) -> None:
        """Handle physical GATT connection event from scheduler/HAL.
        
        Transitions socket physical state to ACTIVE_PHYSICAL_LINK and immediately
        launches the automated transmission pump to flush pending queue bytes (REQ-8).
        """
        async with self._lock:
            sock = self.get_or_create_socket(device_id)
            sock.set_physical_state(SocketState.ACTIVE_PHYSICAL_LINK)

            # Cancel any existing stale pump task for this device
            stale_task = self._flush_tasks.pop(device_id, None)
            if stale_task and not stale_task.done():
                stale_task.cancel()

            # Launch auto-flush pump immediately upon receiving GATT_CONNECTED (REQ-8)
            pump_task = asyncio.create_task(
                self._stream_flush_pump(device_id),
                name=f"flush_pump_{device_id}",
            )
            self._flush_tasks[device_id] = pump_task
            logger.info(
                "SessionManager[%s]: Device '%s' connected; auto-flush pump started (REQ-8)",
                self.session_id,
                device_id,
            )

    async def on_physical_link_paused(self, device_id: str) -> None:
        """Handle TDM slot expiration / intentional rotation disconnect.
        
        Pauses transmission without destroying logical socket (REQ-5).
        Re-queues in-flight unconfirmed chunks to RETRY for subsequent slot (REQ-7).
        """
        async with self._lock:
            sock = self.sockets.get(device_id)
            if sock:
                # Update physical state so while loop in pump exits naturally (REQ-5)
                sock.set_physical_state(SocketState.PAUSED_QUEUED)

            # Wait briefly for in-flight transmission to complete naturally before cancelling
            task = self._flush_tasks.pop(device_id, None)
            if task and not task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=0.015)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            # Preserve any remaining in-flight data by returning unconfirmed chunks to queue (REQ-7)
            buf = self.buffers.get(device_id)
            if buf:
                await buf.requeue_all_in_flight()

            logger.info(
                "SessionManager[%s]: Device '%s' paused for TDM rotation; pending data preserved in RAM",
                self.session_id,
                device_id,
            )

    async def on_physical_link_degraded(self, device_id: str) -> None:
        """Handle connection timeout or link failure (SRS REQ-4).
        
        Flags physical link as DEGRADED_RETRY, stops pump, and requeues pending chunks.
        """
        async with self._lock:
            sock = self.sockets.get(device_id)
            if sock:
                sock.set_physical_state(SocketState.DEGRADED_RETRY)

            task = self._flush_tasks.pop(device_id, None)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            buf = self.buffers.get(device_id)
            if buf:
                await buf.requeue_all_in_flight()

            logger.warning(
                "SessionManager[%s]: Device '%s' marked DEGRADED; pump stopped and chunks queued for retry (REQ-4)",
                self.session_id,
                device_id,
            )

    async def on_chunk_ack_received(self, device_id: str, chunk_id: str) -> bool:
        """Notify buffer of remote ACK confirmation for a dispatched chunk."""
        buf = self.buffers.get(device_id)
        if buf:
            success = await buf.handle_ack(chunk_id)
            if success:
                self.total_completed_chunks += 1
            return success
        return False

    async def on_chunk_nack_or_timeout(self, device_id: str, chunk_id: str) -> bool:
        """Notify buffer of missing ACK or NACK for a dispatched chunk."""
        buf = self.buffers.get(device_id)
        if buf:
            return await buf.handle_nack_or_timeout(chunk_id)
        return False

    async def _stream_flush_pump(self, device_id: str) -> None:
        """Internal asynchronous transmission loop for an active physical radio slot (REQ-8).
        
        Drains raw socket bytes and/or structured chunks and transmits them across
        the physical radio while the physical link remains ACTIVE_PHYSICAL_LINK.
        """
        sock = self.sockets.get(device_id)
        buf = self.buffers.get(device_id)
        if not sock or not buf:
            return

        logger.debug("Flush pump active for device '%s'", device_id)
        try:
            while sock.is_physical_link_active:
                # 1. Non-blocking drain of application raw bytes into structured chunks
                if not sock._tx_queue.empty():
                    raw_bytes = await sock.dequeue_tx(timeout=0)
                    if raw_bytes:
                        chunk = Chunk.create(
                            media_id=f"stream_{device_id}",
                            sequence_number=sock.total_bytes_sent,
                            payload=raw_bytes,
                        )
                        await buf.enqueue(chunk)

                # 2. Dequeue pending chunk ready for transmission immediately
                chunk_to_send = await buf.dequeue_for_transmission(timeout=0)
                if chunk_to_send and self._transport_send_fn:
                    try:
                        # Transmit over HAL callback with shield so rotation disconnect does not kill ACK in-flight
                        sig = inspect.signature(self._transport_send_fn)
                        if len(sig.parameters) >= 3:
                            await asyncio.shield(
                                self._transport_send_fn(device_id, chunk_to_send.payload, chunk_to_send.chunk_id)  # type: ignore
                            )
                        else:
                            await asyncio.shield(self._transport_send_fn(device_id, chunk_to_send.payload))
                    except asyncio.CancelledError:
                        # If cancelled during shield, let caller complete cleanly
                        raise
                    except Exception as err:
                        logger.error(
                            "Transmission failure on device '%s' for chunk '%s': %s",
                            device_id,
                            chunk_to_send.chunk_id,
                            err,
                        )
                        await buf.handle_nack_or_timeout(chunk_to_send.chunk_id)
                elif not chunk_to_send:
                    # If no chunks in buffer, yield briefly before rechecking
                    await asyncio.sleep(0.015)

                # Yield cooperatively
                await asyncio.sleep(0.0015)

        except asyncio.CancelledError:
            logger.debug("Flush pump cancelled for device '%s' (normal slot transition)", device_id)
        except Exception as exc:
            logger.exception("Unexpected error in flush pump for device '%s': %s", device_id, exc)

    async def close(self) -> None:
        """Gracefully terminate all streams and cancel all active pumps."""
        async with self._lock:
            # Cancel all flush tasks
            tasks = [task for task in self._flush_tasks.values() if not task.done()]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._flush_tasks.clear()

            # Close all virtual sockets
            for sock in self.sockets.values():
                await sock.close()

            # Requeue any pending in-flight chunks
            for buf in self.buffers.values():
                await buf.requeue_all_in_flight()

            logger.info("SessionManager[%s]: All streams closed and pumps halted.", self.session_id)
