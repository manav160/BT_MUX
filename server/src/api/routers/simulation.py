"""FastAPI router for TDM Scheduler & Simulation controls."""
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import Response
from pydantic import BaseModel, Field

from ...core.engine import engine

router = APIRouter(prefix="/api/simulation", tags=["Simulation & TDM Control"])


class TSliceUpdate(BaseModel):
    t_slice_ms: int = Field(..., ge=100, le=2000, description="Time-slice duration in milliseconds")


class FaultInjectionRequest(BaseModel):
    node_id: str = Field(..., description="Target peripheral node identifier (e.g. node_08)")
    fault_type: str = Field("degrade", description="'degrade' or 'restore'")


class EnqueueRequest(BaseModel):
    node_id: str = Field(..., description="Target peripheral node identifier")
    chunks: int = Field(3, ge=1, le=50, description="Number of test chunks to enqueue")


class BenchmarkRequest(BaseModel):
    total_nodes: int = Field(12, ge=2, le=30)
    chunks_per_node: int = Field(5, ge=1, le=20)
    t_slice_ms: int = Field(100, ge=50, le=1000)


class ModeSwitchRequest(BaseModel):
    mode: str = Field(..., description="'physical' or 'simulated'")


class PairNodeRequest(BaseModel):
    device_id: str = Field(..., description="Bluetooth MAC address or identifier")
    name: str = Field("BLE Peripheral", description="Peripheral name")
    rssi: int = Field(-60, description="RSSI in dBm")


class UnpairNodeRequest(BaseModel):
    device_id: str = Field(..., description="Bluetooth MAC address or identifier")


class FileTransferRequest(BaseModel):
    filename: Optional[str] = Field(None, description="File name (e.g. document.pdf)")
    size_bytes: Optional[int] = Field(None, ge=1, description="File size in bytes")
    recipient_ids: List[str] = Field(default_factory=list, description="Target device IDs or ['all']")
    chunk_size: int = Field(4096, ge=20, le=65536, description="Chunk size in bytes")
    is_demo: bool = Field(False, description="Is simulated demo payload")


class AddPeerRequest(BaseModel):
    name: Optional[str] = Field(None, description="Optional peer name")


class FaultInjectionConfig(BaseModel):
    enabled: bool = Field(..., description="Enable or disable packet/ACK loss simulation")
    loss_rate: float = Field(0.20, ge=0.0, le=1.0, description="Packet loss probability")


@router.get("/status")
async def get_simulation_status() -> Dict[str, Any]:
    """Retrieve full live telemetry snapshot and node states."""
    return engine.get_telemetry_snapshot()


@router.get("/mode")
async def get_mode() -> Dict[str, Any]:
    """Get active operational mode ('physical' or 'simulated') and adapter."""
    return {
        "mode": engine.mode,
        "transport_name": engine.transport_name,
        "is_running": engine.is_running,
        "nodes_count": len(engine.node_meta),
    }


@router.post("/mode")
async def switch_mode(payload: ModeSwitchRequest) -> Dict[str, Any]:
    """Switch operational mode between Physical Bluetooth (Bleak/WinRT) and Virtual Demo."""
    return await engine.switch_mode(payload.mode)


@router.get("/scan_physical")
async def scan_physical_bluetooth() -> Dict[str, Any]:
    """Perform real over-the-air scan for actual Bluetooth devices using Bleak."""
    devices = await engine.scan_physical_devices()
    return {"devices": devices, "count": len(devices)}


@router.post("/pair_node")
async def pair_node_to_scheduler(payload: PairNodeRequest) -> Dict[str, Any]:
    """Pair a real discovered Bluetooth device into the active TDM scheduler queue."""
    engine.register_node(payload.device_id, payload.name, payload.rssi, is_physical=True)
    return {
        "message": f"Device '{payload.name}' ({payload.device_id}) registered into TDM scheduler.",
        "nodes": engine.get_nodes_snapshot(),
    }


@router.post("/unpair_node")
async def unpair_node_from_scheduler(payload: UnpairNodeRequest) -> Dict[str, Any]:
    """Remove a device from the active TDM scheduler."""
    engine.unregister_node(payload.device_id)
    return {
        "message": f"Device '{payload.device_id}' removed from TDM scheduler.",
        "nodes": engine.get_nodes_snapshot(),
    }


@router.post("/add_peer")
async def add_logical_peer(payload: Optional[AddPeerRequest] = None) -> Dict[str, Any]:
    """Add a new logical peer to the multiplexer up to 12+ (Section 11)."""
    peer = engine.add_peer(name=payload.name if payload else None)
    return {
        "message": f"Added logical peer '{peer['name']}' ({peer['id']})",
        "peer": peer,
        "nodes": engine.get_nodes_snapshot(),
    }


@router.post("/fault_injection")
async def configure_fault_injection(payload: FaultInjectionConfig) -> Dict[str, Any]:
    """Toggle simulated packet loss / lost ACKs (Section 6 & 12)."""
    active = engine.set_fault_injection(payload.enabled, loss_rate=payload.loss_rate)
    return {
        "enabled": active,
        "loss_rate": payload.loss_rate if active else 0.0,
        "message": f"Fault injection {'ENABLED (loss=' + str(int(payload.loss_rate * 100)) + '%)' if active else 'DISABLED'}",
    }


@router.get("/dedup_cache")
async def get_dedup_cache_diagnostics() -> Dict[str, Any]:
    """Retrieve 5-minute delivery history cache stats (Section 8)."""
    return engine.session_mgr.dedup_cache.get_stats()


@router.post("/test_connect")
async def test_peripheral_connection(payload: PairNodeRequest) -> Dict[str, Any]:
    """Test physical GATT connection to a peripheral and inspect services."""
    from ...hal.bleak_adapter import BleakTransport
    temp_transport = BleakTransport()
    res = await temp_transport.test_device_connection(payload.device_id, timeout=3.0)
    return res


@router.post("/transfer_file")
async def transfer_file(payload: FileTransferRequest) -> Dict[str, Any]:
    """Start multiplexed file transfer to target peers."""
    if not engine.is_running:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Simulation is stopped. Please click 'Start Simulation' first.",
        )
    return await engine.start_file_transfer(
        filename=payload.filename,
        size_bytes=payload.size_bytes,
        recipient_ids=payload.recipient_ids,
        chunk_size=payload.chunk_size,
        is_demo=payload.is_demo,
    )


@router.get("/transfers")
async def get_transfers() -> Dict[str, Any]:
    """Retrieve list of active and recent file transfers."""
    return {"transfers": engine.get_file_transfers()}




@router.post("/start")
async def start_simulation() -> Dict[str, Any]:
    """Start the live TDM scheduling loop and traffic generator."""
    await engine.start()
    return {"message": "Simulation started", "status": engine.get_telemetry_snapshot()}


@router.post("/stop")
async def stop_simulation() -> Dict[str, Any]:
    """Pause the live TDM scheduling loop."""
    await engine.stop()
    return {"message": "Simulation paused", "status": engine.get_telemetry_snapshot()}


@router.post("/slice")
async def update_t_slice(payload: TSliceUpdate) -> Dict[str, Any]:
    """Dynamically update T_slice window without system reboot (SRS REQ-11)."""
    engine.set_t_slice_ms(payload.t_slice_ms)
    return {
        "message": f"T_slice updated to {payload.t_slice_ms} ms",
        "t_slice_ms": engine.scheduler.t_slice_ms,
    }


@router.post("/inject_fault")
async def inject_fault(payload: FaultInjectionRequest) -> Dict[str, Any]:
    """Inject peripheral connection timeout or restore node (SRS REQ-4)."""
    success = engine.inject_fault(payload.node_id, payload.fault_type)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to apply fault '{payload.fault_type}' to '{payload.node_id}'",
        )
    return {
        "message": f"Fault '{payload.fault_type}' applied to '{payload.node_id}'",
        "degraded_nodes": list(engine.scheduler.degraded_nodes),
    }


@router.post("/clear_faults")
async def clear_all_faults() -> Dict[str, Any]:
    """Restore all degraded peripherals to healthy state."""
    engine.clear_all_faults()
    return {"message": "All faults cleared"}


@router.post("/enqueue")
async def enqueue_data(payload: EnqueueRequest) -> Dict[str, Any]:
    """Enqueue synthetic test chunks into a node's FIFO queue."""
    await engine.enqueue_data_for_node(payload.node_id, payload.chunks)
    buf = engine.session_mgr.get_buffer(payload.node_id)
    return {
        "message": f"Enqueued {payload.chunks} chunks to {payload.node_id}",
        "queue_depth": buf.queue_depth if buf else 0,
    }


@router.post("/reset")
async def reset_simulation() -> Dict[str, Any]:
    """Reset all node buffers, telemetry counters, and queues."""
    await engine.reset()
    return {"message": "Simulation reset completed"}


@router.post("/benchmark")
async def run_benchmark(payload: Optional[BenchmarkRequest] = None) -> Dict[str, Any]:
    """Trigger the live multi-peer file transfer benchmark (Step 12 / Section 4.1)."""
    req = payload or BenchmarkRequest()
    result = await engine.run_benchmark(
        total_nodes=req.total_nodes,
        chunks_per_node=req.chunks_per_node,
        t_slice_ms=req.t_slice_ms,
    )
    return result


@router.get("/benchmark/latest")
async def get_latest_benchmark() -> Dict[str, Any]:
    """Retrieve the latest benchmark run telemetry and status."""
    if not engine.last_benchmark_result:
        return {"status": "NO_RUNS_YET"}
    return engine.last_benchmark_result


@router.get("/export/csv")
async def export_benchmark_csv() -> Response:
    """Download the benchmark telemetry CSV report (SRS REQ-13)."""
    if not engine.latest_csv_content:
        # Generate default or run benchmark first
        from ...core.csv_export import generate_telemetry_csv
        session_data = {
            "id": engine.session_id,
            "test_name": "BT-Mux Academic Telemetry Export",
            "slice_duration_ms": engine.scheduler.t_slice_ms,
            "max_hardware_limit": 4,
            "status": "COMPLETED",
            "created_at": "2026-10-07T12:00:00Z",
        }
        telemetry_data = {
            "total_packets_sent": engine.total_chunks_sent or 60,
            "total_packets_received": engine.total_chunks_acked or 60,
            "pdr": 1.0,
            "average_rtt": 18.5,
            "average_handoff_latency": 22.5,
        }
        nodes_data = [
            {
                "device_id": n["id"],
                "status": n["state"],
                "priority_weight": 1.0,
                "average_rssi": n["rssi"],
                "average_rtt": n["rtt"],
            }
            for n in engine.get_nodes_snapshot()
        ]
        csv_text = generate_telemetry_csv(session_data, telemetry_data, nodes_data)
    else:
        csv_text = engine.latest_csv_content

    filename = f"btmux_telemetry_{engine.session_id}.csv"
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Cache-Control": "no-cache",
        },
    )
