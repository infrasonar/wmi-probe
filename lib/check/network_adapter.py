from aiowmi.query import Query
from libprobe.asset import Asset
from typing import Tuple
from ..utils import get_state
from ..wmiquery import wmiquery
from ..values import AVAILABILITY_LU, CONFIG_MAN_ERR_CODE, STATUS_INFO


TYPE_NAME = "adapter"
QUERY = Query("""
    SELECT
    AdapterType, AutoSense, Availability, ConfigManagerErrorCode,
    ConfigManagerUserConfig, Description, InstallDate, Installed,
    InterfaceIndex, LastErrorCode, MACAddress, Manufacturer,
    MaxNumberControlled, MaxSpeed, NetConnectionID, NetConnectionStatus,
    NetEnabled, NetworkAddresses, PermanentAddress, PhysicalAdapter,
    PNPDeviceID, PowerManagementSupported, ProductName, ServiceName, Speed,
    Status, StatusInfo, SystemName, TimeOfLastReset
    FROM Win32_NetworkAdapter
""")


def on_item(itm: dict) -> dict:
    itm['name'] = itm.pop('PNPDeviceID')
    return {
        **itm,
        'Availability': AVAILABILITY_LU.get(itm['Availability']),
        'ConfigManagerErrorCode':
            CONFIG_MAN_ERR_CODE.get(itm['ConfigManagerErrorCode']),
        'StatusInfo': STATUS_INFO.get(itm['StatusInfo']),
    }


async def check_network_adapter(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    rows = await wmiquery(asset, asset_config, check_config, QUERY)
    state = get_state(TYPE_NAME, rows, on_item)
    return state