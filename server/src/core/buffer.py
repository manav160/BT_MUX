"""BT-Mux Per-Stream Dedicated RAM FIFO Buffer and Delivery Tracking.

Authoritative source: BT-Mux_Agent_Instructions.md Sections 4.1 & 6, SRS REQ-6, REQ-7.
Maintains dedicated FIFO queues per logical stream to isolate queues and prevent HOL blocking.
"""
import asyncio
import logging
from typing import Dict, Optional

from ..config.constants import ChunkState
from .chunk import Chunk
from .dedup_cache import DedupCache

logger = logging.getLogger("btmux.core.buffer")


class StreamBuffer:
    """Dedicated RAM FIFO buffer queue and delivery tracking for a single logical stream.
    
    Each logical peripheral gets a private StreamBuffer (REQ-6: dedicated queues per stream).
    This design choice prevents Head-of-Line (HOL) blocking: if a peripheral is degraded or
    offline during its slot, its queued chunks stay in its private queue without impeding
    or delaying chunk deliveries to other connected peripherals.
    """

    def __init__(
        self,
        device_id: str,
        dedup_cache: Optional[DedupCache] = None,
        max_queue_size: int = 0,
    ) -> None:
        self.device_id: str = device_id
        # Per-stream FIFO queue (REQ-6)
        self._queue: asyncio.Queue[Chunk] = asyncio.Queue(maxsize=max_queue_size)
        # In-flight chunks that have been sent over radio but await confirmation ACK
        self._in_flight: Dict[str, Chunk] = {}
        # Shared or private dedup cache (Section 4.1)
        self.dedup_cache: DedupCache = dedup_cache or DedupCache()
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def queue_depth(self) -> int:
        """Return number of pending chunks in FIFO queue (SRS REQ-10)."""
        return self._queue.qsize()

    @property
    def in_flight_count(self) -> int:
        """Return number of chunks currently dispatched awaiting remote ACK."""
        return len(self._in_flight)

    async def enqueue(self, chunk: Chunk) -> bool:
        """Enqueue an outgoing chunk into the stream's private FIFO buffer.
        
        Performs dedup check: if chunk was already acknowledged by this device in the
        last 5 minutes (e.g. across a TDM rotation reconnect), it is dropped cleanly
        to preserve radio airtime (Section 4.1).
        
        Args:
            chunk: The transmission chunk.
            
        Returns:
            True if enqueued, False if bypassed as duplicate.
        """
        # Dedup optimization (Section 4.1): avoid re-sending chunks already finished
        if self.dedup_cache.is_completed(self.device_id, chunk.chunk_id) or \
           self.dedup_cache.is_chunk_completed(chunk.transfer_id, chunk.recipient_id or self.device_id, chunk.sequence_number):
            logger.debug(
                "Bypassing duplicate chunk '%s' for device '%s' (found in dedup cache)",
                chunk.chunk_id,
                self.device_id,
            )
            return False

        chunk.state = ChunkState.PENDING
        await self._queue.put(chunk)
        logger.debug(
            "StreamBuffer[%s]: Enqueued chunk '%s' (depth=%d)",
            self.device_id,
            chunk.chunk_id,
            self.queue_depth,
        )
        return True

    async def dequeue_for_transmission(self, timeout: Optional[float] = None) -> Optional[Chunk]:
        """Dequeue the next pending chunk for physical radio transmission.
        
        Transitions chunk state to SENT and places it into the in-flight tracking map.
        Does NOT mark it completed — only explicit ACK reception does that (Section 4.1).
        
        Args:
            timeout: Maximum seconds to wait if queue is currently empty.
            
        Returns:
            Next Chunk ready to transmit, or None if queue is empty / timed out.
        """
        while True:
            try:
                if not self._queue.empty():
                    chunk = self._queue.get_nowait()
                elif timeout is not None and timeout > 0:
                    chunk = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                else:
                    return None
                self._queue.task_done()
            except (asyncio.TimeoutError, asyncio.QueueEmpty):
                return None

            async with self._lock:
                # Lazy skip chunks that reached COMPLETED while waiting in queue (e.g. via late ACK)
                if chunk.state == ChunkState.COMPLETED:
                    continue

                chunk.mark_sent()
                self._in_flight[chunk.chunk_id] = chunk

            logger.debug(
                "StreamBuffer[%s]: Dispatched chunk '%s' to radio (in_flight=%d)",
                self.device_id,
                chunk.chunk_id,
                self.in_flight_count,
            )
            return chunk

    async def handle_ack(self, chunk_id: str) -> bool:
        """Process remote ACK confirming packet arrival.
        
        Advances chunk to COMPLETED and stores in 5-minute dedup cache (Section 4.1).
        Only advance on real confirmation — never mark complete merely on send.
        
        Args:
            chunk_id: Identifier of acknowledged chunk (UUID or compound key).
            
        Returns:
            True if chunk was tracked and acknowledged, False if untracked.
        """
        async with self._lock:
            # Case 1: chunk is in _in_flight
            matched_chunk = None
            if chunk_id in self._in_flight:
                matched_chunk = self._in_flight.pop(chunk_id)
            else:
                for cid, c in list(self._in_flight.items()):
                    compound = self.dedup_cache.make_key(c.transfer_id, c.recipient_id or self.device_id, c.sequence_number)
                    if cid == chunk_id or compound == chunk_id:
                        matched_chunk = self._in_flight.pop(cid)
                        break

            if matched_chunk:
                matched_chunk.mark_completed()
                # Record in delivery history cache (5-minute TTL dedup)
                self.dedup_cache.mark_completed(self.device_id, matched_chunk.chunk_id)
                self.dedup_cache.mark_chunk_completed(
                    matched_chunk.transfer_id,
                    matched_chunk.recipient_id or self.device_id,
                    matched_chunk.sequence_number,
                    matched_chunk.checksum,
                    matched_chunk.size,
                )
                logger.debug(
                    "StreamBuffer[%s]: ACK received for chunk '%s' -> COMPLETED",
                    self.device_id,
                    matched_chunk.chunk_id,
                )
                return True

            # Case 2: chunk is in _queue with state RETRY (late ACK after slot rotation)
            for q_chunk in self._queue._queue:
                compound = self.dedup_cache.make_key(q_chunk.transfer_id, q_chunk.recipient_id or self.device_id, q_chunk.sequence_number)
                if q_chunk.chunk_id == chunk_id or compound == chunk_id:
                    if q_chunk.state == ChunkState.RETRY:
                        q_chunk.mark_completed()
                        self.dedup_cache.mark_completed(self.device_id, q_chunk.chunk_id)
                        self.dedup_cache.mark_chunk_completed(
                            q_chunk.transfer_id,
                            q_chunk.recipient_id or self.device_id,
                            q_chunk.sequence_number,
                            q_chunk.checksum,
                            q_chunk.size,
                        )
                        logger.debug(
                            "StreamBuffer[%s]: Late ACK received for requeued chunk '%s' -> COMPLETED",
                            self.device_id,
                            chunk_id,
                        )
                        return True
                    break

            # Case 3: unknown or already COMPLETED
            return False

    async def handle_nack_or_timeout(self, chunk_id: str) -> bool:
        """Handle missing ACK or transmission timeout.
        
        Transitions chunk state to RETRY and re-enqueues for future slot.
        
        Args:
            chunk_id: Identifier of unacknowledged chunk.
            
        Returns:
            True if chunk was found and queued for retry, False otherwise.
        """
        async with self._lock:
            chunk = self._in_flight.pop(chunk_id, None)
            if not chunk:
                return False

            chunk.mark_retry()

        # Re-queue for next available transmission opportunity
        await self._queue.put(chunk)
        logger.warning(
            "StreamBuffer[%s]: Chunk '%s' unconfirmed -> RETRY (re-enqueued, depth=%d)",
            self.device_id,
            chunk_id,
            self.queue_depth,
        )
        return True

    async def requeue_all_in_flight(self) -> int:
        """Re-enqueue all unconfirmed in-flight chunks to RETRY on slot preemption or disconnect.
        
        Called when physical link is torn down at slice-timer expiry ($T_slice$) so
        any packet in transit without final ACK is retried on next rotation (REQ-7).
        """
        async with self._lock:
            items = list(self._in_flight.values())
            self._in_flight.clear()

        for chunk in items:
            chunk.mark_retry()
            await self._queue.put(chunk)

        if items:
            logger.info(
                "StreamBuffer[%s]: Re-enqueued %d in-flight chunks to RETRY on slot rotation",
                self.device_id,
                len(items),
            )
        return len(items)
