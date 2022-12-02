from aiowmi.query import Query
from libprobe.asset import Asset
from typing import Tuple
from ..utils import get_state
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..values import AVAILABILITY_LU, CONFIG_MAN_ERR_CODE, STATUS_INFO


ADAPTER_TYPE = "adapter"
ADAPTER_QUERY = Query("""
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
INTERFACE_TYPE = "interface"
INTERFACE_QUERY = Query("""
    SELECT
    BytesReceivedPersec, BytesSentPersec, CurrentBandwidth, Name,
    PacketsOutboundDiscarded, PacketsOutboundErrors, PacketsReceivedDiscarded,
    PacketsReceivedErrors, OutputQueueLength
    FROM Win32_PerfFormattedData_Tcpip_NetworkInterface
""")
ROUTE_TYPE = "route"
ROUTE_QUERY = Query("""
    SELECT
    Name, Age, Caption, Description, Destination, Information, InterfaceIndex,
    Mask, Metric1, Metric2, Metric3, Metric4, Metric5, NextHop, Protocol,
    Status, InstallDate, Type
    FROM Win32_IP4RouteTable
""")


def on_item_adapter(itm: dict) -> dict:
    itm['name'] = itm.pop('PNPDeviceID')
    return {
        **itm,
        'Availability': AVAILABILITY_LU.get(itm['Availability']),
        'ConfigManagerErrorCode':
            CONFIG_MAN_ERR_CODE.get(itm['ConfigManagerErrorCode']),
        'StatusInfo': STATUS_INFO.get(itm['StatusInfo']),
    }


def on_item_route(itm: dict) -> dict:
    itm['name'] = '{Destination} [{InterfaceIndex}]'.format_map(itm)
    return itm


async def check_network(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, check_config)
    try:
        rows = await wmiquery(conn, service, ADAPTER_QUERY)
        state = get_state(ADAPTER_TYPE, rows, on_item_adapter)

        rows = await wmiquery(conn, service, INTERFACE_QUERY)
        state.update(get_state(INTERFACE_TYPE, rows))

        rows = await wmiquery(conn, service, ROUTE_QUERY)
        state.update(get_state(ROUTE_TYPE, rows, on_item_route))
    finally:
        wmiclose(conn, service)
    return state
