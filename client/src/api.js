/**
 * BT-Mux API Client
 * Centralized fetch functions for sessions, devices, nodes, and telemetry.
 */
const BASE_URL = 'http://localhost:8000/api';

export async function fetchHealth() {
  const res = await fetch(`${BASE_URL}/health`);
  return res.json();
}

export async function fetchSessions() {
  const res = await fetch(`${BASE_URL}/sessions`);
  return res.json();
}

export async function createSession(payload) {
  const res = await fetch(`${BASE_URL}/sessions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function updateSessionStatus(sessionId, status) {
  const res = await fetch(`${BASE_URL}/sessions/${sessionId}/status`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }),
  });
  return res.json();
}

export async function fetchDevices() {
  const res = await fetch(`${BASE_URL}/devices`);
  return res.json();
}

export async function registerDevice(payload) {
  const res = await fetch(`${BASE_URL}/devices`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function fetchSessionNodes(sessionId) {
  const res = await fetch(`${BASE_URL}/sessions/${sessionId}/nodes`);
  return res.json();
}

export async function addSessionNode(sessionId, payload) {
  const res = await fetch(`${BASE_URL}/sessions/${sessionId}/nodes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function fetchTelemetry(sessionId) {
  const res = await fetch(`${BASE_URL}/sessions/${sessionId}/telemetry`);
  if (!res.ok) return null;
  return res.json();
}

export async function deleteSession(sessionId) {
  const res = await fetch(`${BASE_URL}/sessions/${sessionId}`, { method: 'DELETE' });
  return res.ok;
}

export async function deleteDevice(deviceId) {
  const res = await fetch(`${BASE_URL}/devices/${deviceId}`, { method: 'DELETE' });
  return res.ok;
}

export async function scanDevices(physical = false) {
  const res = await fetch(`${BASE_URL}/devices/scan?physical=${physical}`);
  return res.json();
}

export async function fetchMode() {
  const res = await fetch(`${BASE_URL}/simulation/mode`);
  return res.json();
}

export async function switchMode(mode) {
  const res = await fetch(`${BASE_URL}/simulation/mode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mode }),
  });
  return res.json();
}

export async function scanPhysicalDevices() {
  const res = await fetch(`${BASE_URL}/simulation/scan_physical`);
  return res.json();
}

export async function pairNodeToScheduler(payload) {
  const res = await fetch(`${BASE_URL}/simulation/pair_node`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function unpairNodeFromScheduler(deviceId) {
  const res = await fetch(`${BASE_URL}/simulation/unpair_node`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ device_id: deviceId }),
  });
  return res.json();
}

export async function testConnect(payload) {
  const res = await fetch(`${BASE_URL}/simulation/test_connect`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

// Simulation & Live TDM Scheduler Controls (SRS REQ-2, REQ-4, REQ-11, REQ-13)
export async function fetchSimulationStatus() {
  const res = await fetch(`${BASE_URL}/simulation/status`);
  return res.json();
}

export async function startSimulation() {
  const res = await fetch(`${BASE_URL}/simulation/start`, { method: 'POST' });
  return res.json();
}

export async function stopSimulation() {
  const res = await fetch(`${BASE_URL}/simulation/stop`, { method: 'POST' });
  return res.json();
}

export async function updateTSlice(t_slice_ms) {
  const res = await fetch(`${BASE_URL}/simulation/slice`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ t_slice_ms }),
  });
  return res.json();
}

export async function injectFault(node_id, fault_type = 'degrade') {
  const res = await fetch(`${BASE_URL}/simulation/inject_fault`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ node_id, fault_type }),
  });
  return res.json();
}

export async function clearAllFaults() {
  const res = await fetch(`${BASE_URL}/simulation/clear_faults`, { method: 'POST' });
  return res.json();
}

export async function enqueueTestChunks(node_id, chunks = 3) {
  const res = await fetch(`${BASE_URL}/simulation/enqueue`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ node_id, chunks }),
  });
  return res.json();
}

export async function resetSimulation() {
  const res = await fetch(`${BASE_URL}/simulation/reset`, { method: 'POST' });
  return res.json();
}

export async function runBenchmark(payload = { total_nodes: 12, chunks_per_node: 5, t_slice_ms: 100 }) {
  const res = await fetch(`${BASE_URL}/simulation/benchmark`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function fetchLatestBenchmark() {
  const res = await fetch(`${BASE_URL}/simulation/benchmark/latest`);
  return res.json();
}

export function getBenchmarkCsvUrl() {
  return `${BASE_URL}/simulation/export/csv`;
}

export async function transferFile(payload) {
  const res = await fetch(`${BASE_URL}/simulation/transfer_file`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function fetchTransfers() {
  const res = await fetch(`${BASE_URL}/simulation/transfers`);
  return res.json();
}

export async function addLogicalPeer(name) {
  const res = await fetch(`${BASE_URL}/simulation/add_peer`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  return res.json();
}

export async function setFaultInjection(enabled, loss_rate = 0.20) {
  const res = await fetch(`${BASE_URL}/simulation/fault_injection`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled, loss_rate }),
  });
  return res.json();
}

export async function fetchDedupCache() {
  const res = await fetch(`${BASE_URL}/simulation/dedup_cache`);
  return res.json();
}


