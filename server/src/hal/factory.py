"""Config-Driven Factory for BLE Transport Layer.

Authoritative source: BT-Mux_Hardware_Abstraction_Layer.md Section 4.
This factory is the ONLY place in the codebase that imports concrete adapter classes.
Nowhere else in the system does platform branching occur.
"""
import logging
from typing import Optional

from ..config.settings import get_settings
from .base import BLETransport
from .bleak_adapter import BleakTransport
from .simulated import SimulatedTransport
from ..config.constants import N_MAX_PHYSICAL_DEFAULT

logger = logging.getLogger("btmux.hal.factory")


def create_ble_transport(transport_mode: Optional[str] = None, N_Max: int = N_MAX_PHYSICAL_DEFAULT) -> BLETransport:
    """Create and return a configured BLETransport adapter.
    
    Modes:
    - 'simulated': In-memory simulated transport for local testing and CI (no BLE radio needed)
    - 'winrt': Bleak adapter over Windows WinRT Bluetooth
    - 'linux-bluez': Bleak adapter over Linux BlueZ D-Bus
    
    Args:
        transport_mode: Optional override; defaults to settings.BLE_TRANSPORT.
        
    Returns:
        Instance conforming to BLETransport abstract interface.
    """
    mode = (transport_mode or get_settings().BLE_TRANSPORT).lower().strip()
    logger.info("Initializing BLE transport adapter with mode: '%s'", mode)

    if mode == "simulated":
        return SimulatedTransport()
    elif mode in ("winrt", "linux-bluez"):
        return BleakTransport(max_connections = N_Max)
    else:
        raise ValueError(
            f"Unknown BLE_TRANSPORT mode: '{mode}'. Expected one of ('simulated', 'winrt', 'linux-bluez')"
        )
