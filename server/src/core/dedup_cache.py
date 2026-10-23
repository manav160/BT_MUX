"""BT-Mux In-Memory Delivery-History Dedup Cache.

Authoritative source: BT-Mux_Agent_Instructions.md Section 4.1 & SRS 5.1.
Maintains a short-lived in-memory cache of COMPLETED chunks per device.
"""
import threading
import time
from typing import Dict, Set

from ..config.constants import DEDUP_CACHE_TTL_SECONDS


class DedupCache:
    """In-memory cache tracking recently completed chunks to avoid redundant retransmission.
    
    When a peripheral disconnects and reconnects during TDM slot rotation, this cache
    ensures the host does not resend chunks that were already acknowledged (Section 4.1).
    This cache is purely a dedup optimization in RAM, not persistence. Expired entries
    are purged to guarantee host memory stays well below the 150 MB ceiling (SRS 5.1).
    """

    def __init__(self, ttl_seconds: int = DEDUP_CACHE_TTL_SECONDS) -> None:
        self.ttl_seconds: int = ttl_seconds
        # Structure: {device_id: {chunk_id: completion_monotonic_time}}
        self._cache: Dict[str, Dict[str, float]] = {}
        # Section 8 compound key cache: {compound_key: completion_monotonic_time}
        self._compound_cache: Dict[str, float] = {}
        # Structure: {device_id: {chunk_id: metadata_dict}}
        self._metadata: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # Recent completion events buffer for UI diagnostics
        self._recent_completions: list = []
        self._lock: threading.RLock = threading.RLock()

    @staticmethod
    def make_key(transfer_id: str, recipient_id: str, chunk_sequence: int) -> str:
        """Construct standard Section 8 compound key: transferId + recipientId + chunkSequence."""
        return f"{transfer_id}:{recipient_id}:{chunk_sequence}"

    def mark_completed(
        self,
        device_id: str,
        chunk_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record chunk as completed for target device with current timestamp and metadata."""
        with self._lock:
            now_mono = time.monotonic()
            if device_id not in self._cache:
                self._cache[device_id] = {}
                self._metadata[device_id] = {}
            self._cache[device_id][chunk_id] = now_mono
            meta_record = metadata or {}
            meta_record["completed_at"] = time.time()
            meta_record["device_id"] = device_id
            meta_record["chunk_id"] = chunk_id
            self._metadata[device_id][chunk_id] = meta_record

            self._recent_completions.insert(0, meta_record)
            if len(self._recent_completions) > 50:
                self._recent_completions.pop()

    def mark_chunk_completed(
        self,
        transfer_id: str,
        recipient_id: str,
        chunk_sequence: int,
        checksum: str = "",
        size: int = 0,
        chunk_id: str = "",
    ) -> None:
        """Record chunk completion using Section 8 compound key."""
        with self._lock:
            now_mono = time.monotonic()
            compound_key = self.make_key(transfer_id, recipient_id, chunk_sequence)
            self._compound_cache[compound_key] = now_mono
            meta = {
                "transfer_id": transfer_id,
                "recipient_id": recipient_id,
                "chunk_sequence": chunk_sequence,
                "checksum": checksum,
                "size": size,
                "compound_key": compound_key,
                "completed_at": time.time(),
            }
            if chunk_id:
                self.mark_completed(recipient_id, chunk_id, metadata=meta)

    def is_completed(self, device_id: str, chunk_id: str) -> bool:
        """Check whether chunk was completed recently within TTL window."""
        with self._lock:
            device_entries = self._cache.get(device_id)
            if not device_entries:
                return False

            recorded_time = device_entries.get(chunk_id)
            if recorded_time is None:
                return False

            # Verify whether entry has expired beyond TTL
            if time.monotonic() - recorded_time > self.ttl_seconds:
                # Purge expired entry immediately
                del device_entries[chunk_id]
                if device_id in self._metadata:
                    self._metadata[device_id].pop(chunk_id, None)
                return False

            return True

    def is_chunk_completed(
        self,
        transfer_id: str,
        recipient_id: str,
        chunk_sequence: int,
    ) -> bool:
        """Check compound key completion status."""
        with self._lock:
            compound_key = self.make_key(transfer_id, recipient_id, chunk_sequence)
            recorded_time = self._compound_cache.get(compound_key)
            if recorded_time is None:
                return False
            if time.monotonic() - recorded_time > self.ttl_seconds:
                del self._compound_cache[compound_key]
                return False
            return True

    def get_completed_chunk_ids(self, device_id: str) -> Set[str]:
        """Return set of valid (unexpired) completed chunk IDs for a device."""
        with self._lock:
            self.cleanup_expired()
            return set(self._cache.get(device_id, {}).keys())

    def cleanup_expired(self) -> int:
        """Purge all expired entries across all devices. Returns number of purged entries."""
        with self._lock:
            now = time.monotonic()
            purged = 0
            devices_to_prune = []

            for device_id, chunk_map in self._cache.items():
                expired_keys = [cid for cid, t in chunk_map.items() if now - t > self.ttl_seconds]
                for cid in expired_keys:
                    del chunk_map[cid]
                    if device_id in self._metadata:
                        self._metadata[device_id].pop(cid, None)
                    purged += 1
                if not chunk_map:
                    devices_to_prune.append(device_id)

            for dev_id in devices_to_prune:
                del self._cache[dev_id]
                self._metadata.pop(dev_id, None)

            # Purge compound cache
            expired_compound = [k for k, t in self._compound_cache.items() if now - t > self.ttl_seconds]
            for k in expired_compound:
                del self._compound_cache[k]
                purged += 1

            return purged

    def get_stats(self) -> Dict[str, Any]:
        """Return delivery history cache metrics for UI diagnostics (Section 8)."""
        with self._lock:
            self.cleanup_expired()
            total_entries = sum(len(m) for m in self._cache.values())
            now = time.monotonic()
            all_times = [t for m in self._cache.values() for t in m.values()]
            oldest_age = round(now - min(all_times), 1) if all_times else 0.0

            return {
                "total_records": total_entries,
                "ttl_seconds": self.ttl_seconds,
                "oldest_record_age_s": oldest_age,
                "recent_completions": self._recent_completions[:15],
            }

    def clear(self) -> None:
        """Flush the entire cache."""
        with self._lock:
            self._cache.clear()
            self._compound_cache.clear()
            self._metadata.clear()
            self._recent_completions.clear()
