"""BT-Mux Chunk Data Model and Delivery State Machine.

Authoritative source: BT-Mux_Agent_Instructions.md Section 4.1 & SRS REQ-6.
Governs application-layer multi-peer file transfer chunks.
"""
from dataclasses import dataclass, field
import hashlib
import time
from typing import Any, Dict, Optional
import uuid

from ..config.constants import ChunkState


@dataclass
class Chunk:
    """Application-layer transmission chunk for multi-peer file transfer.

    Fields:
    - media_id: Identifier of file/media payload stream (transferId).
    - chunk_id: Unique identifier for this discrete chunk.
    - sequence_number: Zero-indexed chunk index within the media payload (chunkSequence).
    - payload: Raw payload bytes.
    - size: Payload byte length.
    - checksum: Cryptographic hash (SHA-256) of payload for integrity verification.
    - recipient_id: Logical recipient device identifier (recipientId).
    - attempt_count: Transmission attempt counter (attemptCount).
    - timestamp: Generation epoch timestamp.
    - state: Delivery confirmation state (PENDING -> SENT -> COMPLETED / RETRY).
    """
    media_id: str
    chunk_id: str
    sequence_number: int
    payload: bytes
    size: int
    checksum: str
    recipient_id: str = ""
    attempt_count: int = 0
    timestamp: float = field(default_factory=time.time)
    state: ChunkState = field(default=ChunkState.PENDING)

    @property
    def transfer_id(self) -> str:
        return self.media_id

    @property
    def transferId(self) -> str:
        return self.media_id

    @property
    def recipientId(self) -> str:
        return self.recipient_id

    @property
    def chunk_sequence(self) -> int:
        return self.sequence_number

    @property
    def chunkSequence(self) -> int:
        return self.sequence_number

    @property
    def status(self) -> str:
        return self.state.value

    @property
    def attemptCount(self) -> int:
        return self.attempt_count

    @classmethod
    def create(
        cls,
        media_id: str,
        sequence_number: int,
        payload: bytes,
        chunk_id: Optional[str] = None,
        recipient_id: str = "",
        attempt_count: int = 0,
    ) -> "Chunk":
        """Factory helper creating a chunk with automatic size, hash, and ID computation."""
        cid = chunk_id or uuid.uuid4().hex
        digest = hashlib.sha256(payload).hexdigest()
        return cls(
            media_id=media_id,
            chunk_id=cid,
            sequence_number=sequence_number,
            payload=payload,
            size=len(payload),
            checksum=digest,
            recipient_id=recipient_id,
            attempt_count=attempt_count,
            timestamp=time.time(),
            state=ChunkState.PENDING,
        )

    def mark_sent(self) -> None:
        """Mark chunk as dispatched over radio link (awaiting explicit ACK)."""
        self.state = ChunkState.SENT

    def mark_completed(self) -> None:
        """Mark chunk completed ONLY upon genuine remote ACK reception (Section 4.1)."""
        self.state = ChunkState.COMPLETED

    def mark_retry(self) -> None:
        """Mark chunk for retransmission following ACK timeout or NACK."""
        self.attempt_count += 1
        self.state = ChunkState.RETRY

    def to_dict(self) -> Dict[str, Any]:
        """Convert chunk metadata and payload to dictionary."""
        return {
            "media_id": self.media_id,
            "transferId": self.media_id,
            "transfer_id": self.media_id,
            "chunk_id": self.chunk_id,
            "chunkId": self.chunk_id,
            "sequence_number": self.sequence_number,
            "chunkSequence": self.sequence_number,
            "recipient_id": self.recipient_id,
            "recipientId": self.recipient_id,
            "attempt_count": self.attempt_count,
            "attemptCount": self.attempt_count,
            "size": self.size,
            "checksum": self.checksum,
            "timestamp": self.timestamp,
            "state": self.state.value,
            "status": self.state.value,
        }
