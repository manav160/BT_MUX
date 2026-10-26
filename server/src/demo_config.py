# server/src/demo_config.py

# BLE Scanning Configuration
TARGET_DEVICE_PREFIX = "BTv2_"
SCAN_WAIT_TIME_SEC = 5

# Execution & Timeout Configuration
DEGRADED_TIMEOUT_MS = 9000
T_SLICE_MS = 2000
MAX_LOOP_WAIT_SEC_PER_CHUNK = 1
LOOP_WAIT_STEP = 0.5

# Payload Configuration
FILE_CHUNK_SIZE_BYTES = 20
DEFAULT_TEXT_PAYLOAD_TEMPLATE = "VIVA_{node_id}_{seq}"