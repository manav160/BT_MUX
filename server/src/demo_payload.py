# server/src/demo_payload.py

import os
from typing import List, Dict, Any, Optional
from .core.chunk import Chunk
from .core.session_manager import SessionManager
from .demo_config import FILE_CHUNK_SIZE_BYTES, DEFAULT_TEXT_PAYLOAD_TEMPLATE

async def generate_and_enqueue_chunks(
    session_mgr: SessionManager,
    node_ids: List[str],
    selected_devices: List[Dict[str, Any]],
    file_path: Optional[str] = None,
    default_chunks_per_node: int = 5
) -> int:
    """Reads a file or generates text payloads, chunks them, and enqueues to buffers."""
    total_expected_chunks = 0
    
    for nid in node_ids:
        buf = session_mgr.get_buffer(nid)
        device_name = next((dev['name'] for dev in selected_devices if dev['device_id'] == nid), "Unknown")
        node_short_id = nid[-4:]
        
        payloads = []
        if file_path and os.path.exists(file_path):
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            payloads = [file_bytes[i:i + FILE_CHUNK_SIZE_BYTES] for i in range(0, len(file_bytes), FILE_CHUNK_SIZE_BYTES)]
            print(f"[+] Loaded file: '{file_path}' ({len(file_bytes)} bytes) -> {len(payloads)} chunks")
        else:
            for seq in range(default_chunks_per_node):
                text = DEFAULT_TEXT_PAYLOAD_TEMPLATE.format(node_id=node_short_id, seq=seq, name=device_name)
                payloads.append(text.encode("utf-8"))

        for seq, payload in enumerate(payloads):
            chk = Chunk.create(media_id=f"file_{nid}", sequence_number=seq, payload=payload)
            await buf.enqueue(chk)
            total_expected_chunks += 1
            
    return total_expected_chunks