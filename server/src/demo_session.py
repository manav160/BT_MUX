# server/src/demo_session.py

import time
from typing import List
from .hal.base import BLETransport
from .core.session_manager import SessionManager

def setup_session_and_callback(
    transport: BLETransport,
    node_ids: List[str]
) -> tuple:
    """Initializes SessionManager and returns the Option A ACK callback."""
    session_id = f"SESS_PHYSICAL_{int(time.time())}"
    session_mgr = SessionManager(session_id=session_id)
    ack_count = 0

    async def physical_send_with_ack(device_id: str, data: bytes, chunk_id: str = "") -> None:
        nonlocal ack_count
        try:
            await transport.send(device_id, data)
            if chunk_id:
                if await session_mgr.on_chunk_ack_received(device_id, chunk_id):
                    ack_count += 1
        except Exception as e:
            print(f"    [!] Physical write failed for {device_id[-4:]}: {e}")

    session_mgr.set_transport_send_callback(physical_send_with_ack)
    return session_id, session_mgr