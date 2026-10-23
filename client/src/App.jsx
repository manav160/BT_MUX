import React, { useEffect, useState, useMemo } from 'react';
import { useSchedulerSocket } from './hooks/useSchedulerSocket';
import {
  fetchHealth,
  fetchSimulationStatus,
  startSimulation,
  stopSimulation,
  updateTSlice,
  injectFault,
  clearAllFaults,
  enqueueTestChunks,
  resetSimulation,
  transferFile,
  fetchTransfers,
  addLogicalPeer,
  setFaultInjection,
  fetchDedupCache,
  fetchMode,
  switchMode,
  scanPhysicalDevices,
  pairNodeToScheduler,
  unpairNodeFromScheduler,
  testConnect,
} from './api';
import './App.css';

// Node lifecycle state map matching SRS and UI states
const STATE_META = {
  ACTIVE_PHYSICAL_LINK: { label: 'ACTIVE', className: 'state-active', badgeClass: 'badge-active' },
  ACTIVE:               { label: 'ACTIVE', className: 'state-active', badgeClass: 'badge-active' },
  CONNECTED:            { label: 'CONNECTED', className: 'state-active', badgeClass: 'badge-active' },
  TRANSMITTING:         { label: 'TRANSMITTING', className: 'state-active', badgeClass: 'badge-active' },
  PAUSED_QUEUED:        { label: 'PAUSED / QUEUED', className: 'state-paused', badgeClass: 'badge-paused' },
  PAUSED:               { label: 'PAUSED / QUEUED', className: 'state-paused', badgeClass: 'badge-paused' },
  CONNECTING:           { label: 'CONNECTING', className: 'state-connect', badgeClass: 'badge-connect' },
  DISCONNECTING:        { label: 'DISCONNECTING', className: 'state-connect', badgeClass: 'badge-connect' },
  DEGRADED_RETRY:       { label: 'DEGRADED / TIMEOUT', className: 'state-degraded', badgeClass: 'badge-degraded' },
  DEGRADED:             { label: 'DEGRADED / TIMEOUT', className: 'state-degraded', badgeClass: 'badge-degraded' },
  FAILED:               { label: 'FAILED', className: 'state-degraded', badgeClass: 'badge-degraded' },
  IDLE:                 { label: 'IDLE', className: 'state-uninit', badgeClass: 'badge-uninit' },
  UNINITIALIZED:        { label: 'IDLE', className: 'state-uninit', badgeClass: 'badge-uninit' },
};

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function getRssiQuality(rssi) {
  if (rssi >= -55) return { percent: 100, color: '#5dcaa5', label: 'Excellent' };
  if (rssi >= -68) return { percent: 75, color: '#38bdf8', label: 'Good' };
  if (rssi >= -80) return { percent: 45, color: '#f59e0b', label: 'Fair' };
  return { percent: 20, color: '#f0997b', label: 'Weak' };
}

export default function App() {
  const { isConnected, lastEvent, eventHistory, sendMessage, clearEvents } = useSchedulerSocket('ws://localhost:8000/ws');

  // Navigation tab state: 'FILES' | 'BLUETOOTH'
  const [activeTab, setActiveTab] = useState('FILES');

  // Operational Mode: 'simulated' | 'physical'
  const [transportMode, setTransportMode] = useState('simulated');
  const [transportName, setTransportName] = useState('Simulated BLE (Virtual Demo)');

  // Physical Bluetooth Scanner state
  const [physicalDiscovered, setPhysicalDiscovered] = useState([]);
  const [isPhysicalScanning, setIsPhysicalScanning] = useState(false);
  const [deviceFilter, setDeviceFilter] = useState('');
  const [hideUnnamed, setHideUnnamed] = useState(true);
  const [manualMac, setManualMac] = useState('');
  const [manualName, setManualName] = useState('');
  const [testingDevice, setTestingDevice] = useState(null);
  const [testResultMap, setTestResultMap] = useState({});

  // Scheduler & lifecycle state (Section 2 & 13: starts completely idle)
  const [isSchedulerRunning, setIsSchedulerRunning] = useState(false);
  const [tSliceMs, setTSliceMs] = useState(500);
  const [rotationCount, setRotationCount] = useState(0);
  const [alertMsg, setAlertMsg] = useState(null);
  const [showDebugStream, setShowDebugStream] = useState(false);

  // File Transfer State (Section 2 & 13: starts empty)
  const [fileTransfers, setFileTransfers] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [selectedRecipient, setSelectedRecipient] = useState('all');
  const [isSendingFile, setIsSendingFile] = useState(false);

  // Physical radio slots (Nmax = 4)
  const [activeBatch, setActiveBatch] = useState([]);
  const [queuedBatch, setQueuedBatch] = useState([]);
  const [slotAssignments, setSlotAssignments] = useState(() => [
    { slot_index: 0, slot_number: 1, device_id: null, device_name: 'Idle / Available', state: 'IDLE', is_active: false },
    { slot_index: 1, slot_number: 2, device_id: null, device_name: 'Idle / Available', state: 'IDLE', is_active: false },
    { slot_index: 2, slot_number: 3, device_id: null, device_name: 'Idle / Available', state: 'IDLE', is_active: false },
    { slot_index: 3, slot_number: 4, device_id: null, device_name: 'Idle / Available', state: 'IDLE', is_active: false },
  ]);

  // Telemetry metrics (Section 2 & 10: initial state clean, RTT and PDR null until measured)
  const [ackedChunksMap, setAckedChunksMap] = useState({});
  const [peerRetriesMap, setPeerRetriesMap] = useState({});
  const [inFlightCount, setInFlightCount] = useState(0);
  const [pdr, setPdr] = useState(null);
  const [meanRtt, setMeanRtt] = useState(null);
  const [totalCompletedChunks, setTotalCompletedChunks] = useState(0);
  const [globalRetries, setGlobalRetries] = useState(0);
  const [faultInjectionActive, setFaultInjectionActive] = useState(false);
  const [dedupStats, setDedupStats] = useState(null);

  // Event feed filter for drawer
  const [feedFilter, setFeedFilter] = useState('ALL');

  // Initial logical file peers (Section 2 & 3: 7 configured demo peers, initially IDLE)
  const [nodes, setNodes] = useState(() => [
    { id: 'node_00', name: 'File Peer 00', state: 'IDLE', queueDepth: 0, rssi: -55, rtt: null, is_physical: false },
    { id: 'node_01', name: 'File Peer 01', state: 'IDLE', queueDepth: 0, rssi: -58, rtt: null, is_physical: false },
    { id: 'node_02', name: 'File Peer 02', state: 'IDLE', queueDepth: 0, rssi: -52, rtt: null, is_physical: false },
    { id: 'node_03', name: 'File Peer 03', state: 'IDLE', queueDepth: 0, rssi: -64, rtt: null, is_physical: false },
    { id: 'node_04', name: 'File Peer 04', state: 'IDLE', queueDepth: 0, rssi: -61, rtt: null, is_physical: false },
    { id: 'node_05', name: 'File Peer 05', state: 'IDLE', queueDepth: 0, rssi: -67, rtt: null, is_physical: false },
    { id: 'node_06', name: 'File Peer 06', state: 'IDLE', queueDepth: 0, rssi: -59, rtt: null, is_physical: false },
  ]);

  // Load initial backend telemetry & transfers
  const refreshBackendData = () => {
    fetchMode().then((m) => {
      if (m) {
        if (m.mode) setTransportMode(m.mode);
        if (m.transport_name) setTransportName(m.transport_name);
      }
    }).catch(() => {});

    fetchSimulationStatus().then((status) => {
      if (status) {
        if (status.nodes) setNodes(status.nodes);
        if (status.slots) setSlotAssignments(status.slots);
        if (status.mode) setTransportMode(status.mode);
        if (status.transport_name) setTransportName(status.transport_name);
        if (status.is_running !== undefined) setIsSchedulerRunning(status.is_running);
        if (status.t_slice_ms !== undefined) setTSliceMs(status.t_slice_ms);
        if (status.rotation_count !== undefined) setRotationCount(status.rotation_count);
        if (status.pdr_percent !== undefined) setPdr(status.pdr_percent);
        if (status.mean_rtt_ms !== undefined) setMeanRtt(status.mean_rtt_ms);
        if (status.in_flight_chunks !== undefined) setInFlightCount(status.in_flight_chunks);
        if (status.total_chunks_acked !== undefined) setTotalCompletedChunks(status.total_chunks_acked);
        if (status.retry_count !== undefined) setGlobalRetries(status.retry_count);
        if (status.fault_injection_active !== undefined) setFaultInjectionActive(status.fault_injection_active);
        if (status.dedup_cache_stats) setDedupStats(status.dedup_cache_stats);
      }
    }).catch(() => {});

    fetchTransfers().then((res) => {
      if (res?.transfers) setFileTransfers(res.transfers);
    }).catch(() => {});
  };

  useEffect(() => {
    refreshBackendData();
  }, []);

  const showAlert = (msg) => {
    setAlertMsg(msg);
    setTimeout(() => setAlertMsg(null), 4500);
  };

  // Process Real-time WebSocket Events
  useEffect(() => {
    if (!lastEvent) return;

    if (lastEvent.type === 'SLOT_ROTATION' && lastEvent.data) {
      if (lastEvent.data.active_batch) setActiveBatch(lastEvent.data.active_batch);
      if (lastEvent.data.queued_batch) setQueuedBatch(lastEvent.data.queued_batch);
      if (lastEvent.data.rotation_count !== undefined) setRotationCount(lastEvent.data.rotation_count);
      if (lastEvent.data.t_slice_ms !== undefined) setTSliceMs(lastEvent.data.t_slice_ms);
    } else if (lastEvent.type === 'SOCKET_STATE_CHANGE' && lastEvent.data) {
      const { device_id, state } = lastEvent.data;
      setNodes((prev) => prev.map((n) => (n.id === device_id ? { ...n, state } : n)));
    } else if (lastEvent.type === 'BUFFER_UPDATE' && lastEvent.data) {
      const { device_id, queue_depth } = lastEvent.data;
      setNodes((prev) => prev.map((n) => (n.id === device_id ? { ...n, queueDepth: queue_depth } : n)));
    } else if (lastEvent.type === 'CHUNK_EVENT' && lastEvent.data?.device_id) {
      const devId = lastEvent.data.device_id;
      setAckedChunksMap((prev) => ({ ...prev, [devId]: (prev[devId] || 0) + 1 }));
    } else if (lastEvent.type === 'FILE_TRANSFER_START' && lastEvent.data) {
      setFileTransfers((prev) => [lastEvent.data, ...prev.filter((t) => t.id !== lastEvent.data.id)]);
    } else if (lastEvent.type === 'FILE_TRANSFER_PROGRESS' && lastEvent.data) {
      setFileTransfers((prev) =>
        prev.map((t) => (t.id === lastEvent.data.id ? { ...t, ...lastEvent.data } : t))
      );
    } else if (lastEvent.type === 'CONFIG_UPDATE' && lastEvent.data?.t_slice_ms) {
      setTSliceMs(lastEvent.data.t_slice_ms);
    } else if (lastEvent.type === 'MODE_CHANGED' && lastEvent.data) {
      setTransportMode(lastEvent.data.mode);
      setTransportName(lastEvent.data.transport_name);
    } else if (lastEvent.type === 'TELEMETRY_SNAPSHOT' && lastEvent.data) {
      const snap = lastEvent.data;
      if (snap.nodes) setNodes(snap.nodes);
      if (snap.slots) setSlotAssignments(snap.slots);
      if (snap.rotation_count !== undefined) setRotationCount(snap.rotation_count);
      if (snap.is_running !== undefined) setIsSchedulerRunning(snap.is_running);
      if (snap.t_slice_ms !== undefined) setTSliceMs(snap.t_slice_ms);
      if (snap.pdr_percent !== undefined) setPdr(snap.pdr_percent);
      if (snap.mean_rtt_ms !== undefined) setMeanRtt(snap.mean_rtt_ms);
      if (snap.in_flight_chunks !== undefined) setInFlightCount(snap.in_flight_chunks);
      if (snap.total_chunks_acked !== undefined) setTotalCompletedChunks(snap.total_chunks_acked);
      if (snap.retry_count !== undefined) setGlobalRetries(snap.retry_count);
      if (snap.fault_injection_active !== undefined) setFaultInjectionActive(snap.fault_injection_active);
      if (snap.dedup_cache_stats) setDedupStats(snap.dedup_cache_stats);
      if (snap.mode) setTransportMode(snap.mode);
      if (snap.transport_name) setTransportName(snap.transport_name);
    } else if (lastEvent.type === 'RESET_EVENT') {
      setIsSchedulerRunning(false);
      setRotationCount(0);
      setTotalCompletedChunks(0);
      setGlobalRetries(0);
      setInFlightCount(0);
      setMeanRtt(null);
      setPdr(null);
      setFileTransfers([]);
      setSelectedFile(null);
      setAckedChunksMap({});
      setPeerRetriesMap({});
      clearEvents();
    }
  }, [lastEvent]);

  // Periodic poll of 5-minute dedup cache stats
  useEffect(() => {
    const timer = setInterval(() => {
      fetchDedupCache().then((res) => { if (res) setDedupStats(res); }).catch(() => {});
    }, 4000);
    return () => clearInterval(timer);
  }, []);

  // Mode Switcher Handler: 'simulated' vs 'physical'
  const handleModeSwitch = async (targetMode) => {
    try {
      const res = await switchMode(targetMode);
      setTransportMode(res.mode);
      setTransportName(res.transport_name);
      if (res.nodes) setNodes(res.nodes);
      showAlert(
        targetMode === 'physical'
          ? '📶 Switched to Physical BLE Mode (Bleak/WinRT). Hardware scanner & link probing ready. Real file transfer implementation deferred to Phase 2.'
          : '🧪 Switched to Simulated BLE Mode. Full software-only virtual testbed active.'
      );
      if (targetMode === 'physical') {
        setActiveTab('BLUETOOTH');
        handleScanPhysical();
      }
    } catch (e) {
      showAlert(`Mode switch failed: ${e.message}`);
    }
  };

  // Scan real physical Bluetooth devices using Bleak
  const handleScanPhysical = async () => {
    setIsPhysicalScanning(true);
    showAlert('Scanning Bluetooth spectrum via host radio (Bleak)...');
    try {
      const res = await scanPhysicalDevices();
      const devs = res.devices || [];
      setPhysicalDiscovered(devs);
      showAlert(`Discovered ${devs.length} Bluetooth devices in radio range!`);
    } catch (e) {
      showAlert(`Physical BLE scan error: ${e.message}`);
    } finally {
      setIsPhysicalScanning(false);
    }
  };

  // Pair a physical Bluetooth peripheral into TDM Scheduler
  const handlePairPhysical = async (dev) => {
    try {
      const res = await pairNodeToScheduler({
        device_id: dev.device_id,
        name: dev.name || 'BLE Peripheral',
        rssi: dev.rssi || -60,
      });
      if (res.nodes) setNodes(res.nodes);
      showAlert(`Added real device ${dev.name || dev.device_id} to TDM scheduler as logical peer.`);
    } catch (e) {
      showAlert(`Pairing error: ${e.message}`);
    }
  };

  // Unpair / remove device from TDM Scheduler
  const handleUnpairNode = async (deviceId) => {
    try {
      const res = await unpairNodeFromScheduler(deviceId);
      if (res.nodes) setNodes(res.nodes);
      showAlert(`Removed device ${deviceId} from logical peer pool.`);
    } catch (e) {
      showAlert(`Unpair error: ${e.message}`);
    }
  };

  // Manual MAC address pair
  const handleManualPair = async (e) => {
    e.preventDefault();
    if (!manualMac.trim()) return;
    await handlePairPhysical({
      device_id: manualMac.trim(),
      name: manualName.trim() || 'Physical BLE Peripheral',
      rssi: -65,
    });
    setManualMac('');
    setManualName('');
  };

  // Test physical GATT link connectivity
  const handleTestConnect = async (dev) => {
    setTestingDevice(dev.device_id);
    try {
      const res = await testConnect({
        device_id: dev.device_id,
        name: dev.name || 'BLE Peripheral',
        rssi: dev.rssi || -60,
      });
      setTestResultMap((prev) => ({ ...prev, [dev.device_id]: res }));
      if (res.success) {
        showAlert(`✓ Connection probe passed for ${dev.name || dev.device_id}! Found ${res.services_count} GATT services.`);
      } else {
        showAlert(`ℹ ${res.message || 'Link test completed. Peripheral in standby.'}`);
      }
    } catch (e) {
      setTestResultMap((prev) => ({
        ...prev,
        [dev.device_id]: { success: false, message: e.message },
      }));
      showAlert(`Connection probe error: ${e.message}`);
    } finally {
      setTestingDevice(null);
    }
  };

  // Quick add all top discovered devices
  const handleQuickAddAll = async () => {
    const toAdd = physicalDiscovered.slice(0, 8);
    for (const dev of toAdd) {
      await handlePairPhysical(dev);
    }
    showAlert(`Registered top ${toAdd.length} discovered devices into TDM multiplexer!`);
  };

  // Lifecycle Control Handlers (Section 3 & 12)
  const handleStartSimulation = async () => {
    try {
      const res = await startSimulation();
      setIsSchedulerRunning(true);
      if (res?.status) {
        if (res.status.nodes) setNodes(res.status.nodes);
        if (res.status.slots) setSlotAssignments(res.status.slots);
      }
      showAlert('Simulation started! Logical peers sharing 4 physical slots via round-robin TDM.');
    } catch (e) {
      showAlert(`Start simulation error: ${e.message}`);
    }
  };

  const handlePauseSimulation = async () => {
    try {
      await stopSimulation();
      setIsSchedulerRunning(false);
      showAlert('Simulation paused. State, slots, and pending chunk queues preserved.');
    } catch (e) {
      showAlert(`Pause error: ${e.message}`);
    }
  };

  const handleReset = async () => {
    try {
      await resetSimulation();
      setIsSchedulerRunning(false);
      setRotationCount(0);
      setTotalCompletedChunks(0);
      setGlobalRetries(0);
      setInFlightCount(0);
      setMeanRtt(null);
      setPdr(null);
      setFileTransfers([]);
      setSelectedFile(null);
      setAckedChunksMap({});
      setPeerRetriesMap({});
      clearEvents();
      refreshBackendData();
      showAlert('System reset: scheduler stopped, 4 physical slots IDLE, queues & cache restored to clean initial state.');
    } catch (e) {
      showAlert(`Reset error: ${e.message}`);
    }
  };

  // Dynamic T_slice update handler (Section 5 & 14)
  const handleTSliceChange = async (val) => {
    setTSliceMs(val);
    try {
      await updateTSlice(val);
      showAlert(`TDM slot duration updated to ${val} ms (T_slice)`);
    } catch {
      sendMessage({ command: 'SET_T_SLICE', value: val });
    }
  };

  // Fault injection toggle (Section 10)
  const handleToggleFault = async () => {
    try {
      const targetRate = faultInjectionActive ? 0.0 : 0.35;
      const res = await setFaultInjection(!faultInjectionActive, targetRate);
      setFaultInjectionActive(res.enabled);
      showAlert(
        res.enabled
          ? '⚡ Fault Injection Enabled (35% simulated packet/ACK loss). Watch retry attempts & late ACK deduplication!'
          : '✓ Fault Injection Disabled. Normal zero-loss operation restored.'
      );
    } catch (e) {
      showAlert(`Fault injection error: ${e.message}`);
    }
  };

  // Node degrade handler
  const handleFaultInjection = async (nodeId, type) => {
    try {
      await injectFault(nodeId, type);
      showAlert(`Logical peer ${nodeId} transitioned to ${type === 'degrade' ? 'DEGRADED_RETRY (Timeout)' : 'RESTORED'}`);
    } catch (e) {
      showAlert(`Fault injection failed: ${e.message}`);
    }
  };

  const handleClearFaults = async () => {
    await clearAllFaults();
    showAlert('All peer link timeouts cleared. Peers restored to queue.');
  };

  // Enqueue test chunks to a specific peer
  const handleEnqueue = async (nodeId, chunks = 3) => {
    if (!isSchedulerRunning) {
      showAlert('Simulation is stopped. Please click "▶ Start Simulation" first.');
      return;
    }
    await enqueueTestChunks(nodeId, chunks);
    showAlert(`Enqueued ${chunks} chunks into ${nodeId} FIFO queue.`);
  };

  // Add a new logical peer stream (Section 11)
  const handleAddPeer = async () => {
    try {
      const res = await addLogicalPeer();
      if (res.nodes) setNodes(res.nodes);
      showAlert(`Added logical stream ${res.peer?.id || 'new'} (${res.nodes?.length || nodes.length + 1} total peers multiplexing across 4 physical slots)`);
    } catch (e) {
      showAlert(`Add peer error: ${e.message}`);
    }
  };

  // Remove peer cleanly
  const handleRemovePeer = async (nodeId) => {
    await handleUnpairNode(nodeId);
  };

  // Sample file selections
  const handlePickSampleFile = (name, sizeBytes, isDemo = true) => {
    setSelectedFile({
      name,
      size: sizeBytes,
      type: 'sample/file',
      is_demo: isDemo,
    });
  };

  // Send Demo Payload (100 KB) prominent control
  const handleSendDemoPayload = async () => {
    if (!isSchedulerRunning) {
      showAlert('Simulation is stopped. Please click "▶ Start Simulation" first.');
      return;
    }
    const recipients = selectedRecipient === 'all' ? ['all'] : [selectedRecipient];
    try {
      setIsSendingFile(true);
      const res = await transferFile({
        filename: 'Generated_Demo_Payload.bin',
        size_bytes: 100 * 1024,
        recipient_ids: recipients,
        chunk_size: 4096,
        is_demo: true,
      });
      setFileTransfers((prev) => [res, ...prev.filter((t) => t.id !== res.id)]);
      showAlert(`Demo payload (100 KB) queued across ${res.recipients.length} peer(s).`);
    } catch (e) {
      showAlert(`Transfer error: ${e.message}`);
    } finally {
      setIsSendingFile(false);
    }
  };

  // File Transfer submission
  const handleSendFile = async () => {
    if (!isSchedulerRunning) {
      showAlert('Simulation is stopped. Please click "▶ Start Simulation" first.');
      return;
    }
    const fileToTransfer = selectedFile || {
      name: 'Generated_Demo_Payload.bin',
      size: 100 * 1024,
      type: 'sample/demo',
      is_demo: true,
    };
    const recipients = selectedRecipient === 'all' ? ['all'] : [selectedRecipient];
    try {
      setIsSendingFile(true);
      const res = await transferFile({
        filename: fileToTransfer.name,
        size_bytes: fileToTransfer.size,
        recipient_ids: recipients,
        chunk_size: 4096,
        is_demo: fileToTransfer.is_demo || false,
      });
      setFileTransfers((prev) => [res, ...prev.filter((t) => t.id !== res.id)]);
      showAlert(`Transfer started: "${fileToTransfer.name}" (${formatBytes(fileToTransfer.size)}) to ${res.recipients.length} peer(s).`);
      setSelectedFile(null);
    } catch (e) {
      showAlert(`File transfer error: ${e.message}`);
    } finally {
      setIsSendingFile(false);
    }
  };

  const handleFileSelect = (e) => {
    const file = e.target.files?.[0];
    if (file) {
      setSelectedFile({
        name: file.name,
        size: file.size,
        type: file.type || 'application/octet-stream',
        is_demo: false,
      });
    }
  };

  const handleRefreshDedup = async () => {
    try {
      const res = await fetchDedupCache();
      if (res) setDedupStats(res);
    } catch {}
  };

  // Filtered event history for debug drawer
  const filteredEvents = useMemo(() => {
    if (feedFilter === 'ALL') return eventHistory;
    return eventHistory.filter((ev) => {
      if (feedFilter === 'CONNECTION') return ev.type === 'SOCKET_STATE_CHANGE' || ev.type === 'MODE_CHANGED';
      if (feedFilter === 'SCHEDULING') return ev.type === 'SLOT_ROTATION' || ev.type === 'CONFIG_UPDATE';
      if (feedFilter === 'QUEUES') return ev.type === 'BUFFER_UPDATE';
      if (feedFilter === 'CHUNKS') return ev.type === 'CHUNK_EVENT' || ev.type === 'FILE_TRANSFER_PROGRESS';
      if (feedFilter === 'ACKS') return ev.type === 'CHUNK_EVENT' && ev.data?.status === 'ACKED';
      if (feedFilter === 'FAILURES') return ev.data?.status === 'RETRY' || ev.data?.status === 'FAILED' || ev.data?.state === 'DEGRADED_RETRY';
      return true;
    });
  }, [eventHistory, feedFilter]);

  // Telemetry Aggregates
  const activeNodes = useMemo(
    () => (isSchedulerRunning ? nodes.filter((n) => n.state === 'ACTIVE_PHYSICAL_LINK' || n.state === 'ACTIVE' || n.state === 'CONNECTED' || n.state === 'TRANSMITTING') : []),
    [nodes, isSchedulerRunning]
  );
  const activePhysicalCount = slotAssignments.filter((s) => s.state !== 'IDLE' && s.device_id).length;

  const totalQueueDepth = useMemo(
    () => nodes.reduce((acc, curr) => acc + (curr.queueDepth || 0), 0),
    [nodes]
  );

  const activeNodeIds = useMemo(() => new Set(nodes.map((n) => n.id)), [nodes]);

  const filteredDiscovered = useMemo(() => {
    return physicalDiscovered.filter((d) => {
      if (hideUnnamed && !d.has_name) return false;
      if (!deviceFilter.trim()) return true;
      const q = deviceFilter.toLowerCase();
      return (
        (d.name && d.name.toLowerCase().includes(q)) ||
        (d.device_id && d.device_id.toLowerCase().includes(q))
      );
    });
  }, [physicalDiscovered, hideUnnamed, deviceFilter]);

  const namedCount = useMemo(
    () => physicalDiscovered.filter((d) => d.has_name).length,
    [physicalDiscovered]
  );

  return (
    <div className="dashboard-container">
      {/* Top Banner / System Header */}
      <header className="dashboard-header">
        <div className="brand-section">
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <h1 className="brand-title">
              <span>📂</span> BT-Mux Diagnostic Profiler & Multiplexer
            </h1>
            <span
              className={`node-badge ${
                transportMode === 'physical'
                  ? 'badge-physical'
                  : isSchedulerRunning
                  ? 'badge-active'
                  : rotationCount > 0
                  ? 'badge-paused'
                  : 'badge-uninit'
              }`}
              style={{ fontSize: '0.74rem', padding: '3px 9px', letterSpacing: '0.04em' }}
            >
              {transportMode === 'physical'
                ? '📶 PHYSICAL BLE (STANDBY)'
                : isSchedulerRunning
                ? '● SIMULATION RUNNING'
                : rotationCount > 0
                ? '⏸ PAUSED'
                : '○ SIMULATION READY'}
            </span>
          </div>
          <div className="header-subtitle">
            Software-based BLE multiplexing architecture — dual-mode simulation & physical scanner
          </div>
        </div>

        <div className="header-status">
          {/* Mode Selector Switcher */}
          <div className="mode-toggle-group" title="Switch between Simulated BLE and Physical Bluetooth modes">
            <button
              type="button"
              className={`mode-toggle-btn ${transportMode === 'simulated' ? 'active mode-simulated' : ''}`}
              onClick={() => handleModeSwitch('simulated')}
            >
              🧪 Simulated BLE
            </button>
            <button
              type="button"
              className={`mode-toggle-btn ${transportMode === 'physical' ? 'active mode-physical' : ''}`}
              onClick={() => handleModeSwitch('physical')}
            >
              📶 Physical BLE
            </button>
          </div>

          {!isSchedulerRunning ? (
            <button className="btn btn-sm btn-primary" onClick={handleStartSimulation}>
              ▶ Start Simulation
            </button>
          ) : (
            <button className="btn btn-sm btn-secondary" onClick={handlePauseSimulation}>
              ⏸ Pause
            </button>
          )}

          <button
            className="btn btn-sm btn-accent"
            onClick={handleSendDemoPayload}
            title="Generate and transmit synthetic 100 KB binary payload"
          >
            🚀 Send Demo Payload (100 KB)
          </button>

          <button className="btn btn-sm btn-outline" onClick={handleReset} title="Reset to clean idle state">
            ↺ Reset
          </button>

          <button
            className={`btn btn-sm btn-outline ${showDebugStream ? 'btn-active-debug' : ''}`}
            onClick={() => setShowDebugStream(!showDebugStream)}
            title="Inspect runtime event log"
          >
            🛠️ Logs ({eventHistory.length})
          </button>
        </div>
      </header>

      {/* Navigation Tabs Bar (SRS Requirement) */}
      <nav className="nav-tabs-bar">
        <button
          className={`nav-tab-btn ${activeTab === 'FILES' ? 'active' : ''}`}
          onClick={() => setActiveTab('FILES')}
        >
          📁 File Transfer & Dashboard
        </button>
        <button
          className={`nav-tab-btn ${activeTab === 'BLUETOOTH' ? 'active' : ''}`}
          onClick={() => setActiveTab('BLUETOOTH')}
        >
          📡 Bluetooth Device Scanner
          {physicalDiscovered.length > 0 && (
            <span className="brand-tag" style={{ marginLeft: '4px', fontSize: '0.65rem', padding: '1px 6px' }}>
              {physicalDiscovered.length}
            </span>
          )}
        </button>
      </nav>

      {/* Main Grid Workspace */}
      <main className="dashboard-body">
        {/* Transient Alert Banner */}
        {alertMsg && (
          <div className="alert-banner alert-success">
            <span>{alertMsg}</span>
            <button
              onClick={() => setAlertMsg(null)}
              style={{ background: 'none', border: 'none', color: 'inherit', cursor: 'pointer' }}
            >
              ✕
            </button>
          </div>
        )}

        {/* TAB 1: FILE TRANSFER & MULTIPLEXING DASHBOARD */}
        {activeTab === 'FILES' && (
          <div className="main-panel">
            {/* Simulation Architecture Status Details Bar */}
            <div className="sim-status-details-bar">
              <div className="status-detail-item">
                <span className="status-detail-label">Hardware Limit:</span>
                <span className="status-detail-val">4 Physical Slots (Nmax)</span>
              </div>
              <div className="status-detail-item">
                <span className="status-detail-label">Logical Streams:</span>
                <span className="status-detail-val">
                  {isSchedulerRunning ? nodes.length : 0} Active ({nodes.length} Configured)
                </span>
              </div>
              <div className="status-detail-item">
                <span className="status-detail-label">Scheduler:</span>
                <span className="status-detail-val">Round-Robin TDM</span>
              </div>
              <div className="status-detail-item">
                <span className="status-detail-label">Time Slice:</span>
                <span className="status-detail-val">{tSliceMs} ms (T_slice)</span>
              </div>
              <div className="status-detail-item">
                <span className="status-detail-label">Active Transport:</span>
                <span className="status-detail-val">{transportName}</span>
              </div>
            </div>

            {/* Mode Banner */}
            <div className="sim-hardware-banner">
              <div className="sim-banner-left">
                <span className="sim-banner-pill">
                  {transportMode === 'physical'
                    ? 'PHYSICAL BLE MODE — STANDBY'
                    : isSchedulerRunning
                    ? 'SIMULATED BLE — TDM ENGINE ACTIVE'
                    : 'SIMULATION READY — NO HARDWARE REQUIRED'}
                </span>
                <span className="sim-banner-sub">
                  {transportMode === 'physical'
                    ? 'Real hardware scanner and link probing active via Bleak. Real data transmission implementation is deferred to Phase 2.'
                    : isSchedulerRunning
                    ? `TDM scheduler active: multiplexing ${nodes.length} virtual streams across 4 simulated slots with sequential ACK confirmation`
                    : 'Click "Start Simulation" to begin round-robin slot rotation across virtual peers node_00 to node_06'}
                </span>
              </div>
              <div className="sim-banner-actions">
                <button
                  className={`btn btn-sm ${faultInjectionActive ? 'btn-fault active' : 'btn-outline'}`}
                  onClick={handleToggleFault}
                  title="Inject 35% packet loss to verify timeout, retry & late ACK deduplication"
                >
                  {faultInjectionActive ? '⚡ Loss Simulation: 35% ACTIVE' : '⚡ Simulate Packet Loss'}
                </button>
                <button
                  className="btn btn-sm btn-outline"
                  onClick={handleAddPeer}
                  title="Scale to 12+ logical streams sharing the 4 physical slots"
                >
                  + Add Logical Peer ({nodes.length})
                </button>
                <button
                  className="btn btn-sm btn-outline"
                  onClick={() => {
                    handleRefreshDedup();
                    setShowDebugStream(true);
                  }}
                  title="View 5-minute RAM delivery history and completion cache"
                >
                  📦 5-Min Cache ({dedupStats?.total_entries || 0})
                </button>
              </div>
            </div>

            {/* Architecture Summary */}
            <div className="arch-summary">
              <span className="arch-flow">
                <strong>{isSchedulerRunning ? nodes.length : 0}</strong> logical streams
                <span className="arch-arrow">→</span>
                independent FIFO queues ({totalQueueDepth} chunks)
                <span className="arch-arrow">→</span>
                TDM scheduler ({tSliceMs}ms T_slice)
                <span className="arch-arrow">→</span>
                <strong>4</strong> physical slots (Nmax)
                <span className="arch-arrow">→</span>
                Transport Abstraction ({transportMode === 'physical' ? 'Bleak' : 'SimulatedTransport'})
              </span>
            </div>

            {/* Key Metrics Banner (10 Core Metrics) */}
            <section className="metrics-banner">
              <div className="metric-card" title="Physical connections active (Max Nmax = 4)">
                <span className="metric-title">Active Physical Slots</span>
                <span className="metric-val">{isSchedulerRunning ? activePhysicalCount : 0}<span className="metric-of">/4</span></span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Physical Radio Slots</span>
              </div>
              <div className="metric-card" title="Logical streams sharing the 4 physical slots">
                <span className="metric-title">Logical Streams</span>
                <span className="metric-val">{isSchedulerRunning ? nodes.length : 0}</span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>{nodes.length} Configured</span>
              </div>
              <div className="metric-card" title="Chunks currently in flight awaiting ACK confirmation">
                <span className="metric-title">In-Flight Chunks</span>
                <span className="metric-val" style={{ color: inFlightCount > 0 ? 'var(--accent-cyan)' : undefined }}>
                  {inFlightCount}
                </span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Awaiting ACK</span>
              </div>
              <div className="metric-card" title="TDM round-robin rotation count">
                <span className="metric-title">Rotations</span>
                <span className="metric-val">{rotationCount}</span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Batch Cycles</span>
              </div>
              <div className="metric-card" title="TDM time slice duration per batch window">
                <span className="metric-title">Time Slice (T_slice)</span>
                <span className="metric-val" style={{ color: 'var(--accent-cyan)' }}>
                  {tSliceMs}<span className="metric-unit">ms</span>
                </span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Window Limit</span>
              </div>
              <div className="metric-card" title="Mean ACK round-trip time">
                <span className="metric-title">Mean ACK RTT</span>
                <span className="metric-val">{meanRtt != null ? `${meanRtt} ms` : '—'}</span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Link Latency</span>
              </div>
              <div className="metric-card" title="Packet Delivery Ratio based on acknowledged vs transmitted chunks">
                <span className="metric-title">PDR</span>
                <span className="metric-val" style={{ color: pdr != null && pdr >= 95 ? 'var(--state-active)' : undefined }}>
                  {pdr != null ? `${pdr}%` : '—'}
                </span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>ACK Delivery Basis</span>
              </div>
              <div className="metric-card" title="Total ACK-confirmed chunks completed">
                <span className="metric-title">Completed Chunks</span>
                <span className="metric-val">{totalCompletedChunks}</span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Confirmed ACKs</span>
              </div>
              <div className="metric-card" title="Retries triggered on unacknowledged or lost chunks">
                <span className="metric-title">Retries</span>
                <span className="metric-val" style={{ color: globalRetries > 0 ? 'var(--state-degraded)' : undefined }}>
                  {globalRetries}
                </span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>Retry Events</span>
              </div>
              <div className="metric-card" title="Total active entries in 5-minute RAM deduplication cache">
                <span className="metric-title">5-Min Cache</span>
                <span className="metric-val" style={{ color: 'var(--accent-cyan)' }}>
                  {dedupStats?.total_entries || 0}
                </span>
                <span style={{ fontSize: '0.68rem', color: 'var(--text-muted)' }}>RAM Records</span>
              </div>
            </section>

            {/* SECTION: Four Physical Radio Slot Cards */}
            <section className="radio-slots-card">
              <div className="card-title-simple">
                <span>Physical Radio Slots (Nmax = 4 Slots)</span>
                <span className={`status-dot ${isSchedulerRunning ? 'live' : 'paused'}`}>
                  {isSchedulerRunning ? '● TDM Active' : '○ Idle'}
                </span>
              </div>

              <div className="radio-slots-grid">
                {[0, 1, 2, 3].map((slotIdx) => {
                  const slotInfo = slotAssignments && slotAssignments[slotIdx];
                  const activeNodeId = isSchedulerRunning && slotInfo ? slotInfo.device_id : null;
                  const slotState = isSchedulerRunning && slotInfo ? slotInfo.state : 'IDLE';
                  const dev = activeNodeId
                    ? nodes.find((n) => n.id === activeNodeId) || { id: activeNodeId, name: activeNodeId, queueDepth: 0, rssi: -60, rtt: null }
                    : null;

                  return (
                    <div key={slotIdx} className={`radio-slot-box ${dev && slotState !== 'IDLE' ? 'slot-active' : 'slot-idle'}`}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <span className="slot-channel-tag">Slot {slotIdx + 1}</span>
                        <span
                          className={`node-badge ${
                            slotState === 'CONNECTED' || slotState === 'TRANSMITTING' || slotState === 'ACTIVE_PHYSICAL_LINK'
                              ? 'badge-active'
                              : slotState === 'CONNECTING' || slotState === 'DISCONNECTING'
                              ? 'badge-connect'
                              : slotState === 'FAILED' || slotState === 'DEGRADED_RETRY'
                              ? 'badge-degraded'
                              : 'badge-uninit'
                          }`}
                          style={{ fontSize: '0.65rem' }}
                        >
                          {slotState}
                        </span>
                      </div>
                      {dev && slotState !== 'IDLE' ? (
                        <>
                          <span className="slot-device-name">{dev.name}</span>
                          <span className="slot-device-meta">
                            {dev.rssi} dBm · {dev.rtt != null ? `${dev.rtt} ms` : '—'} · {dev.queueDepth} queued
                          </span>
                          {slotInfo?.current_transfer && (
                            <div className="slot-transfer-info">
                              <span>📦 {slotInfo.current_transfer}</span>
                              {slotInfo.current_chunk_info && <span> · {slotInfo.current_chunk_info}</span>}
                            </div>
                          )}
                        </>
                      ) : (
                        <span className="slot-device-name" style={{ opacity: 0.4 }}>Idle / Available</span>
                      )}
                    </div>
                  );
                })}
              </div>
            </section>

            {/* SECTION: Logical Stream Peers Panel */}
            <section className="canvas-card">
              <div className="card-title-simple">
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span>Logical Stream Peers ({nodes.length})</span>
                  <span style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                    Sharing 4 Physical Slots via Round-Robin TDM
                  </span>
                </div>
                <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                  <button className="btn btn-sm btn-outline" onClick={handleAddPeer}>
                    + Add Logical Peer
                  </button>
                  <button className="btn btn-sm btn-outline" onClick={() => nodes.forEach((n) => handleEnqueue(n.id, 2))}>
                    + Send 2 Chunks to All
                  </button>
                  {nodes.some((n) => n.state === 'DEGRADED_RETRY' || n.state === 'DEGRADED') && (
                    <button className="btn btn-sm btn-outline" onClick={handleClearFaults}>
                      ✓ Recover All
                    </button>
                  )}
                </div>
              </div>

              <div className="nodes-grid">
                {nodes.map((node) => {
                  const meta = STATE_META[node.state] || STATE_META.UNINITIALIZED;
                  const slotIndex = isSchedulerRunning && slotAssignments.length > 0
                    ? slotAssignments.findIndex((s) => s.device_id === node.id && s.state !== 'IDLE')
                    : -1;
                  const isDegraded = node.state === 'DEGRADED_RETRY' || node.state === 'DEGRADED' || node.state === 'FAILED';

                  return (
                    <div
                      key={node.id}
                      className={`node-card ${slotIndex !== -1 ? 'node-card-active' : ''}`}
                      style={{ borderLeftColor: isDegraded ? 'var(--state-degraded)' : undefined }}
                    >
                      <div className="node-card-header">
                        <strong className="node-name">{node.name}</strong>
                        <span className={`node-badge ${meta.badgeClass}`}>{meta.label}</span>
                      </div>

                      {slotIndex !== -1 ? (
                        <div className="slot-indicator">Physical Slot #{slotIndex + 1}</div>
                      ) : isSchedulerRunning ? (
                        <div style={{ fontSize: '0.68rem', color: 'var(--text-muted)', marginBottom: '4px' }}>
                          Queued (Logical Stream Kept)
                        </div>
                      ) : (
                        <div style={{ fontSize: '0.68rem', color: 'var(--text-muted)', marginBottom: '4px' }}>
                          Idle (Simulation Stopped)
                        </div>
                      )}

                      <div className="node-quick-stats">
                        <span>{node.rssi} dBm</span>
                        <span>·</span>
                        <span>{node.rtt != null ? `${node.rtt} ms` : '—'}</span>
                        <span>·</span>
                        <span>{node.acked_chunks || ackedChunksMap[node.id] || 0} ACK</span>
                        <span>·</span>
                        <span style={{ color: (node.retries || peerRetriesMap[node.id] || 0) > 0 ? 'var(--state-degraded)' : undefined }}>
                          {node.retries || peerRetriesMap[node.id] || 0} Retries
                        </span>
                      </div>

                      <div className="buffer-meter">
                        <div className="buffer-label">
                          <span>FIFO Buffer Queue</span>
                          <span>{node.queueDepth || 0} chunks</span>
                        </div>
                        <div className="meter-track">
                          <div
                            className="meter-fill"
                            style={{ width: `${Math.min(100, ((node.queueDepth || 0) / 10) * 100)}%` }}
                          />
                        </div>
                      </div>

                      <div className="node-actions-row">
                        <button
                          className="btn btn-sm btn-outline"
                          onClick={() => handleEnqueue(node.id, 3)}
                          title="Queue 3 synthetic file chunks to this peer's FIFO"
                        >
                          +3 Chunks
                        </button>
                        <button
                          className="btn btn-sm btn-outline"
                          onClick={() => handleFaultInjection(node.id, isDegraded ? 'restore' : 'degrade')}
                          title={isDegraded ? 'Recover node link' : 'Simulate signal loss / timeout'}
                        >
                          {isDegraded ? '✓ Recover' : '⚡ Degrade'}
                        </button>
                        <button
                          className="btn btn-sm btn-outline"
                          style={{ color: 'var(--text-muted)' }}
                          onClick={() => handleRemovePeer(node.id)}
                          title="Remove this logical peer from the scheduler pool"
                        >
                          ✕ Remove
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>

            {/* SECTION: File Transfer Center & Progress */}
            <section className="file-transfer-section">
              <div className="card-title-simple">
                <span>📂 BLE File Transfer Center</span>
                <span style={{ fontSize: '0.8rem', color: 'var(--accent-cyan)' }}>
                  Multiplexed Concurrent File Delivery
                </span>
              </div>

              <div className="file-transfer-grid">
                {/* Left Column: Send File */}
                <div>
                  <h3 style={{ fontSize: '0.9rem', color: 'var(--text-primary)', marginBottom: '8px' }}>
                    Select File to Send
                  </h3>

                  <label className="file-dropzone" htmlFor="file-input-upload">
                    <input
                      id="file-input-upload"
                      type="file"
                      style={{ display: 'none' }}
                      onChange={handleFileSelect}
                    />
                    <div className="file-dropzone-icon">📤</div>
                    <div style={{ fontSize: '0.86rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                      Click to Choose File or Drag & Drop
                    </div>
                    <div style={{ fontSize: '0.74rem', color: 'var(--text-secondary)', marginTop: '4px' }}>
                      Auto-chunked @ 4 KB synthetic chunks (simulated payload delivery)
                    </div>
                  </label>

                  {selectedFile && (
                    <div className="file-selected-box">
                      <div>
                        <strong style={{ fontSize: '0.85rem', color: 'var(--text-primary)' }}>
                          📄 {selectedFile.name}
                        </strong>
                        <div style={{ fontSize: '0.74rem', color: 'var(--text-secondary)' }}>
                          {formatBytes(selectedFile.size)} · {Math.max(1, Math.ceil(selectedFile.size / 4096))} BLE Chunks
                        </div>
                      </div>
                      <button
                        className="btn btn-sm btn-outline"
                        onClick={() => setSelectedFile(null)}
                      >
                        ✕ Clear
                      </button>
                    </div>
                  )}

                  <div style={{ marginTop: '12px' }}>
                    <span style={{ fontSize: '0.74rem', color: 'var(--text-muted)' }}>Or pick a built-in sample payload:</span>
                    <div className="sample-files-row">
                      <button
                        className="sample-file-chip"
                        onClick={() => handlePickSampleFile('Firmware_Update_v2.bin', 48 * 1024, true)}
                      >
                        ⚙️ Firmware (48 KB)
                      </button>
                      <button
                        className="sample-file-chip"
                        onClick={() => handlePickSampleFile('Diagnostic_Report.pdf', 24 * 1024, true)}
                      >
                        📄 Report (24 KB)
                      </button>
                      <button
                        className="sample-file-chip"
                        onClick={() => handlePickSampleFile('Sensor_Capture.jpg', 64 * 1024, true)}
                      >
                        📷 Image (64 KB)
                      </button>
                      <button
                        className="sample-file-chip"
                        onClick={() => handlePickSampleFile('Generated_Demo_Payload.bin', 100 * 1024, true)}
                      >
                        ⚡ Demo Payload (100 KB)
                      </button>
                    </div>
                  </div>

                  <div style={{ marginTop: '16px' }}>
                    <label style={{ fontSize: '0.78rem', color: 'var(--text-secondary)', display: 'block', marginBottom: '6px' }}>
                      Recipient Peer:
                    </label>
                    <select
                      className="config-input"
                      style={{ width: '100%', marginBottom: '12px' }}
                      value={selectedRecipient}
                      onChange={(e) => setSelectedRecipient(e.target.value)}
                    >
                      <option value="all">📡 All File Peers (Multicast to {nodes.length} nodes)</option>
                      {nodes.map((n) => (
                        <option key={n.id} value={n.id}>
                          {n.name} ({n.id})
                        </option>
                      ))}
                    </select>

                    <button
                      className="btn btn-primary"
                      style={{ width: '100%', padding: '10px' }}
                      onClick={handleSendFile}
                      disabled={isSendingFile}
                    >
                      {isSendingFile
                        ? '⏳ Enqueueing Chunks...'
                        : selectedFile
                        ? `🚀 Send "${selectedFile.name}" via Multiplexer`
                        : '🚀 Send Demo Payload (100 KB)'}
                    </button>
                  </div>
                </div>

                {/* Right Column: Active & Recent Transfers */}
                <div>
                  <h3 style={{ fontSize: '0.9rem', color: 'var(--text-primary)', marginBottom: '8px' }}>
                    Transfer Queue & History ({fileTransfers.length})
                  </h3>

                  {fileTransfers.length === 0 ? (
                    <div style={{ background: 'var(--bg-surface)', padding: '32px 16px', borderRadius: '8px', textAlign: 'center', border: '1px solid var(--border)' }}>
                      <div style={{ fontSize: '1.8rem', marginBottom: '8px' }}>📂</div>
                      <p style={{ color: 'var(--text-muted)', fontSize: '0.82rem', margin: '0 0 12px 0' }}>
                        No active transfers. Click "Start Simulation" and then "Send Demo Payload (100 KB)" to watch TDM chunk multiplexing in real time.
                      </p>
                      <button
                        className="btn btn-sm btn-primary"
                        onClick={isSchedulerRunning ? handleSendDemoPayload : handleStartSimulation}
                      >
                        {isSchedulerRunning ? '🚀 Launch Demo Payload (100 KB)' : '▶ Start Simulation First'}
                      </button>
                    </div>
                  ) : (
                    <div className="transfer-list">
                      {fileTransfers.map((tx) => {
                        const percent = tx.total_chunks > 0 ? Math.min(100, Math.round((tx.chunks_acked / tx.total_chunks) * 100)) : 0;
                        const isDone = tx.status === 'COMPLETED' || percent >= 100;
                        const isMulticast = tx.recipients && tx.recipients.length > 1;

                        return (
                          <div key={tx.id} className={`transfer-item ${isDone ? 'completed' : ''}`}>
                            <div className="transfer-item-header">
                              <span className="transfer-filename">
                                {isDone ? '✓' : '🔄'} {tx.filename}
                                {tx.is_demo && (
                                  <span style={{ fontSize: '0.68rem', marginLeft: '6px', color: 'var(--accent-cyan)', background: 'rgba(56,189,248,0.1)', padding: '1px 5px', borderRadius: '4px' }}>
                                    DEMO PAYLOAD
                                  </span>
                                )}
                              </span>
                              <span
                                className={`node-badge ${isDone ? 'badge-physical' : 'badge-active'}`}
                                style={{ fontSize: '0.7rem' }}
                              >
                                {isDone ? 'Completed' : `${percent}%`}
                              </span>
                            </div>

                            <div className="progress-bar-track">
                              <div
                                className={`progress-bar-fill ${isDone ? 'done' : ''}`}
                                style={{ width: `${percent}%` }}
                              />
                            </div>

                            <div className="transfer-meta-row">
                              <span>
                                {formatBytes(tx.size_bytes)} · {tx.chunks_acked}/{tx.total_chunks} Chunks
                                {tx.retries_count > 0 && ` · ${tx.retries_count} Retries`}
                              </span>
                              <span>
                                {tx.speed_kbps > 0 ? `${tx.speed_kbps} KB/s · ` : ''}
                                {tx.recipients?.length === 1 ? `Peer: ${tx.recipient_names?.[0] || tx.recipients[0]}` : `Multicast to ${tx.recipients?.length || 0} Peers`}
                              </span>
                            </div>

                            {/* Independent per-recipient multicast tracking */}
                            {isMulticast && tx.recipient_progress && (
                              <div className="multicast-tray">
                                {Object.entries(tx.recipient_progress).map(([rId, p]) => {
                                  const rPercent = p.total_chunks > 0 ? Math.round((p.chunks_acked / p.total_chunks) * 100) : 0;
                                  const rDone = p.status === 'COMPLETED' || rPercent >= 100;
                                  return (
                                    <span key={rId} className={`multicast-peer-chip ${rDone ? 'done' : ''}`}>
                                      {rDone ? '✓' : '🔄'} {rId}: {p.chunks_acked}/{p.total_chunks} ({rPercent}%)
                                    </span>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>
            </section>

            {/* SECTION: 5-Minute Delivery History Cache */}
            <section className="canvas-card dedup-card">
              <div className="card-title-simple">
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span>📦 5-Minute Delivery History Cache (Deduplication Engine)</span>
                  <span style={{ fontSize: '0.72rem', color: 'var(--accent-cyan)' }}>
                    RAM In-Memory Cache (TTL: 300s)
                  </span>
                </div>
                <button className="btn btn-sm btn-outline" onClick={handleRefreshDedup}>
                  ↻ Refresh Cache
                </button>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px', marginBottom: '14px' }}>
                <div style={{ background: 'var(--bg-card)', padding: '10px 14px', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Key Structure</div>
                  <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--accent-cyan)' }}>
                    transferId:recipientId:chunkSeq
                  </div>
                </div>
                <div style={{ background: 'var(--bg-card)', padding: '10px 14px', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Cached Chunk Records</div>
                  <div style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-primary)' }}>
                    {dedupStats?.total_entries || 0}
                  </div>
                </div>
                <div style={{ background: 'var(--bg-card)', padding: '10px 14px', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Cached Recipient Devices</div>
                  <div style={{ fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-primary)' }}>
                    {dedupStats?.devices_cached || 0}
                  </div>
                </div>
                <div style={{ background: 'var(--bg-card)', padding: '10px 14px', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>Cache Eviction Policy</div>
                  <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)' }}>
                    Non-blocking TTL 300s
                  </div>
                </div>
              </div>

              {dedupStats?.recent_entries && dedupStats.recent_entries.length > 0 ? (
                <div style={{ overflowX: 'auto' }}>
                  <table className="dedup-recent-table">
                    <thead>
                      <tr>
                        <th>Compound Cache Key</th>
                        <th>Recipient</th>
                        <th>Status</th>
                        <th>Time Cached</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dedupStats.recent_entries.slice(0, 6).map((item, idx) => (
                        <tr key={idx}>
                          <td><code>{item.key}</code></td>
                          <td>{item.device_id}</td>
                          <td><span className="node-badge badge-active">ACK_CONFIRMED</span></td>
                          <td style={{ color: 'var(--text-muted)' }}>
                            {item.timestamp ? new Date(item.timestamp * 1000).toLocaleTimeString() : 'Recent'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', textAlign: 'center', padding: '12px' }}>
                  No records in the 5-minute deduplication RAM cache yet. Entries will appear as chunks are confirmed by ACKs.
                </div>
              )}
            </section>

            {/* TDM Slot Duration Controls */}
            <section className="canvas-card">
              <div className="tslice-compact">
                <span className="tslice-compact-label">Slot Duration (T_slice):</span>
                <div className="tslice-preset-chips">
                  {[100, 250, 500, 1000].map((preset) => (
                    <button
                      key={preset}
                      className={`tslice-chip ${tSliceMs === preset ? 'active' : ''}`}
                      onClick={() => handleTSliceChange(preset)}
                    >
                      {preset}ms
                    </button>
                  ))}
                </div>
                <input
                  type="range"
                  min="100"
                  max="1500"
                  step="50"
                  value={tSliceMs}
                  onChange={(e) => handleTSliceChange(Number(e.target.value))}
                  className="slider-input"
                  style={{ flex: 1, maxWidth: '200px' }}
                />
                <span style={{ fontSize: '0.85rem', fontWeight: 700, color: 'var(--accent-cyan)' }}>
                  {tSliceMs} ms
                </span>
              </div>
            </section>
          </div>
        )}

        {/* TAB 2: BLUETOOTH DEVICE SCANNER (Physical & Simulated BLE Device Management) */}
        {activeTab === 'BLUETOOTH' && (
          <div className="scanner-container">
            {/* Guide Banner */}
            <div className="guide-card">
              <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '8px' }}>
                <span style={{ fontSize: '1.4rem' }}>📡</span>
                <div>
                  <h3 style={{ fontSize: '0.98rem', fontWeight: 700, margin: 0, color: 'var(--text-primary)' }}>
                    Bluetooth Device Scanner & Peer Management
                  </h3>
                  <p style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', margin: 0 }}>
                    Discover real Bluetooth peripherals using your host radio (Bleak) or manage virtual simulated peers
                  </p>
                </div>
              </div>

              <div className="guide-steps-grid">
                <div className="guide-step-box">
                  <span className="guide-step-num">Step 1 &bull; Scan</span>
                  <span className="guide-step-title">📡 Discover Nearby Devices</span>
                  <div className="guide-step-desc">
                    Click <strong>Scan Bluetooth Devices</strong> to query your host PC radio via Bleak (WinRT) for advertising BLE peripherals.
                  </div>
                </div>

                <div className="guide-step-box">
                  <span className="guide-step-num">Step 2 &bull; Pair</span>
                  <span className="guide-step-title">🔗 Add to Logical Peers</span>
                  <div className="guide-step-desc">
                    Click <strong>+ Add as Peer</strong> on any discovered device to assign it an independent FIFO queue and round-robin TDM slot.
                  </div>
                </div>

                <div className="guide-step-box">
                  <span className="guide-step-num">Step 3 &bull; Multiplex</span>
                  <span className="guide-step-title">📂 Share Radio Slots</span>
                  <div className="guide-step-desc">
                    Switch to the <strong>File Transfer</strong> tab to watch the scheduler time-multiplex all logical streams across 4 physical slots.
                  </div>
                </div>
              </div>
            </div>

            {/* Hardware Separation Notice Banner */}
            <div className="filter-info-banner" style={{ background: 'rgba(56, 189, 248, 0.08)', borderColor: 'rgba(56, 189, 248, 0.3)' }}>
              <span>
                <strong>ℹ Architecture Notice:</strong> Physical BLE scanning and GATT service probing interface with your host Bluetooth controller via Bleak. Real over-the-air file transmission will be integrated in Phase 2. Simulated mode provides full end-to-end chunk transmission, ACKs, retries, and deduplication.
              </span>
            </div>

            {/* Scanner Controls & Grid */}
            <section className="scanner-card">
              <div className="card-title-simple">
                <span>Bluetooth Device Scanner</span>
                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                  <button
                    className="btn btn-primary"
                    onClick={() => {
                      if (transportMode !== 'physical') handleModeSwitch('physical');
                      else handleScanPhysical();
                    }}
                    disabled={isPhysicalScanning}
                  >
                    {isPhysicalScanning ? '🔍 Scanning Radio Spectrum...' : '📡 Scan Bluetooth Devices'}
                  </button>
                  {physicalDiscovered.length > 0 && (
                    <button
                      className="btn btn-secondary"
                      onClick={handleQuickAddAll}
                    >
                      ⚡ Add Top {Math.min(8, physicalDiscovered.length)} as Peers
                    </button>
                  )}
                  {transportMode === 'simulated' && (
                    <button className="btn btn-outline" onClick={handleAddPeer}>
                      + Add Simulated Peer
                    </button>
                  )}
                </div>
              </div>

              <p style={{ fontSize: '0.82rem', color: 'var(--text-secondary)', lineHeight: '1.5', marginTop: '6px' }}>
                Discover nearby BLE devices and register them as logical communication streams. The round-robin TDM engine shares the 4 physical radio slots fairly among all peers.
              </p>

              {/* Toolbar: Search input + Filter pills */}
              <div className="scanner-toolbar">
                <input
                  type="text"
                  className="scanner-search-input"
                  placeholder="🔍 Filter devices by name or MAC address (e.g. Phone, Buds, ESP32)..."
                  value={deviceFilter}
                  onChange={(e) => setDeviceFilter(e.target.value)}
                />
                <div className="scanner-filter-toggles">
                  <button
                    type="button"
                    className={`filter-pill-btn ${hideUnnamed ? 'active' : ''}`}
                    onClick={() => setHideUnnamed(true)}
                  >
                    ⭐ Named Only ({namedCount})
                  </button>
                  <button
                    type="button"
                    className={`filter-pill-btn ${!hideUnnamed ? 'active' : ''}`}
                    onClick={() => setHideUnnamed(false)}
                  >
                    📡 All ({physicalDiscovered.length})
                  </button>
                </div>
              </div>

              {/* Filter notice */}
              {hideUnnamed && physicalDiscovered.length > namedCount && (
                <div className="filter-info-banner">
                  <span>
                    🛡️ Showing <strong>{filteredDiscovered.length} named devices</strong>. Filtered out {physicalDiscovered.length - namedCount} unnamed beacons and privacy MACs.
                  </span>
                  <button
                    type="button"
                    className="filter-info-btn"
                    onClick={() => setHideUnnamed(false)}
                  >
                    Show all {physicalDiscovered.length} devices
                  </button>
                </div>
              )}

              {/* Manual MAC address pairing input */}
              <form onSubmit={handleManualPair} style={{ display: 'flex', gap: '8px', margin: '12px 0 16px 0', flexWrap: 'wrap' }}>
                <input
                  type="text"
                  className="config-input"
                  style={{ flex: 1, minWidth: '200px' }}
                  placeholder="Enter MAC address (e.g. AA:BB:CC:11:22:33)..."
                  value={manualMac}
                  onChange={(e) => setManualMac(e.target.value)}
                />
                <input
                  type="text"
                  className="config-input"
                  style={{ width: '180px' }}
                  placeholder="Device Name (optional)"
                  value={manualName}
                  onChange={(e) => setManualName(e.target.value)}
                />
                <button type="submit" className="btn btn-secondary">
                  + Add MAC
                </button>
              </form>

              {/* Discovered BLE Peripherals Grid */}
              <div>
                {isPhysicalScanning ? (
                  <div style={{ background: 'var(--bg-surface)', padding: '36px', borderRadius: '8px', textAlign: 'center', border: '1px solid var(--border)' }}>
                    <div style={{ fontSize: '2rem', marginBottom: '8px' }}>📡</div>
                    <h4 style={{ color: 'var(--accent-cyan)', margin: '0 0 6px 0' }}>Scanning Bluetooth Spectrum...</h4>
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.8rem', margin: 0 }}>
                      Bleak adapter is querying host Bluetooth radio for advertising devices...
                    </p>
                  </div>
                ) : filteredDiscovered.length === 0 ? (
                  <div style={{ background: 'var(--bg-surface)', padding: '28px', borderRadius: '8px', textAlign: 'center', border: '1px solid var(--border)' }}>
                    <p style={{ color: 'var(--text-muted)', marginBottom: '12px' }}>
                      {physicalDiscovered.length === 0
                        ? 'No devices scanned yet. Click "Scan Bluetooth Devices" to discover nearby BLE hardware.'
                        : 'No devices match your search filter.'}
                    </p>
                    <button className="btn btn-primary" onClick={handleScanPhysical} disabled={isPhysicalScanning}>
                      Start Real Bluetooth Scan Now
                    </button>
                  </div>
                ) : (
                  <div className="physical-devices-grid">
                    {filteredDiscovered.map((dev) => {
                      const isAlreadyPaired = activeNodeIds.has(dev.device_id);
                      const testResult = testResultMap[dev.device_id];
                      const isTesting = testingDevice === dev.device_id;
                      const quality = getRssiQuality(dev.rssi || -65);

                      return (
                        <div key={dev.device_id} className="physical-device-card">
                          <div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '6px' }}>
                              <strong style={{ fontSize: '0.92rem', color: 'var(--text-primary)' }}>
                                {dev.name || 'BLE Device'}
                              </strong>
                              <span className="node-badge badge-physical">BLE</span>
                            </div>
                            <div style={{ marginBottom: '8px' }}>
                              <span className="mac-pill">{dev.device_id}</span>
                            </div>

                            {/* RSSI Signal Bar */}
                            <div className="rssi-indicator">
                              <span style={{ color: quality.color, fontWeight: 600 }}>
                                📶 {dev.rssi || -65} dBm
                              </span>
                              <div className="rssi-bar-outer">
                                <div
                                  className="rssi-bar-inner"
                                  style={{ width: `${quality.percent}%`, background: quality.color }}
                                />
                              </div>
                              <span style={{ color: 'var(--text-muted)', fontSize: '0.7rem' }}>
                                ({quality.label})
                              </span>
                            </div>

                            {/* Test Link Result feedback */}
                            {testResult && (
                              <div style={{
                                marginTop: '8px',
                                padding: '5px 8px',
                                borderRadius: '4px',
                                fontSize: '0.72rem',
                                lineHeight: '1.3',
                                background: testResult.success ? 'rgba(93, 202, 165, 0.12)' : 'rgba(240, 153, 123, 0.12)',
                                border: `1px solid ${testResult.success ? 'var(--state-active)' : 'var(--state-degraded)'}`,
                                color: testResult.success ? 'var(--state-active)' : 'var(--text-primary)',
                              }}>
                                {testResult.success
                                  ? `✓ Connected (${testResult.services_count} GATT Services discovered)`
                                  : `ℹ ${testResult.message || 'Connection test finished'}`}
                              </div>
                            )}
                          </div>

                          <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', marginTop: '12px' }}>
                            <div style={{ display: 'flex', gap: '6px' }}>
                              <button
                                className="btn btn-outline"
                                style={{ flex: 1, fontSize: '0.72rem', padding: '6px' }}
                                onClick={() => handleTestConnect(dev)}
                                disabled={isTesting}
                                title="Probe physical GATT connectivity"
                              >
                                {isTesting ? '⚡ Probing...' : '⚡ Test Link'}
                              </button>
                              {isAlreadyPaired ? (
                                <button
                                  className="btn btn-secondary"
                                  style={{ flex: 2, fontSize: '0.72rem', padding: '6px' }}
                                  onClick={() => handleUnpairNode(dev.device_id)}
                                >
                                  ✓ Added (Remove)
                                </button>
                              ) : (
                                <button
                                  className="btn btn-primary"
                                  style={{ flex: 2, fontSize: '0.72rem', padding: '6px' }}
                                  onClick={() => handlePairPhysical(dev)}
                                >
                                  ➕ Add as Logical Peer
                                </button>
                              )}
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </section>
          </div>
        )}
      </main>

      {/* Off-Canvas Slide-Over Event Log Console */}
      {showDebugStream && (
        <div className="debug-drawer-overlay" onClick={() => setShowDebugStream(false)}>
          <aside className="debug-drawer-panel" onClick={(e) => e.stopPropagation()}>
            <div className="feed-card debug-drawer-card">
              <div className="card-title">
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span>🛠️ System Event Log</span>
                  <span style={{ fontSize: '0.72rem', color: 'var(--accent-cyan)' }}>
                    {filteredEvents.length} events
                  </span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <button
                    className="btn btn-outline"
                    style={{ fontSize: '0.68rem', padding: '2px 8px' }}
                    onClick={clearEvents}
                    title="Clear event feed history"
                  >
                    🗑️ Clear
                  </button>
                  <button
                    className="btn btn-outline"
                    style={{ fontSize: '0.75rem', padding: '2px 8px' }}
                    onClick={() => setShowDebugStream(false)}
                    title="Close debug console"
                  >
                    ✕
                  </button>
                </div>
              </div>

              {/* Feed Filter Chips */}
              <div className="feed-filter-chips">
                {['ALL', 'CONNECTION', 'SCHEDULING', 'QUEUES', 'CHUNKS', 'ACKS', 'FAILURES'].map((filterKey) => (
                  <button
                    key={filterKey}
                    className={`feed-filter-chip ${feedFilter === filterKey ? 'active' : ''}`}
                    onClick={() => setFeedFilter(filterKey)}
                  >
                    {filterKey}
                  </button>
                ))}
              </div>

              <div className="feed-list">
                {filteredEvents.length === 0 ? (
                  <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                    No events captured in current run. Click "Start Simulation" or send chunks to populate.
                  </div>
                ) : (
                  filteredEvents.map((ev, i) => (
                    <div key={i} className="feed-item">
                      <div className="feed-item-header">
                        <span className="feed-type">{ev.type}</span>
                        <span className="feed-time">{ev.timestamp || ev.time}</span>
                      </div>
                      <div className="feed-data">
                        {JSON.stringify(ev.data, null, 2)}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </aside>
        </div>
      )}
    </div>
  );
}
