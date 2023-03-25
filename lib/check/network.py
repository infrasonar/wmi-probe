from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..counters import on_counters
from ..utils import get_state
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..values import (
    CONFIG_MAN_ERR_CODE,
    NET_CONN_STATUS,
    ROUTE_PROTOCOL_MAP,
    ROUTE_TYPE_MAP,
)

ADAPTER_TYPE = "adapter"
ADAPTER_QUERY = Query("""
    SELECT
    AdapterType, AutoSense, ConfigManagerErrorCode, MACAddress, Manufacturer,
    NetConnectionID, NetConnectionStatus, NetEnabled, PhysicalAdapter,
    PNPDeviceID, ProductName, ServiceName, Speed
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
CACHE = {}
INTERFACE_TYPE_RAW = "interfaceCounters"
INTERFACE_QUERY_RAW = Query("""
    SELECT
    BytesReceivedPersec, BytesSentPersec, CurrentBandwidth, Name,
    PacketsOutboundDiscarded, PacketsOutboundErrors, PacketsReceivedDiscarded,
    PacketsReceivedErrors, OutputQueueLength,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_NetworkInterface
""")
ROUTE_TYPE = "route"
ROUTE_QUERY = Query("""
    SELECT
    Destination, InterfaceIndex, Mask, Metric1, NextHop, Protocol, Type
    FROM Win32_IP4RouteTable
""")


def on_item_adapter(itm: dict) -> dict:
    itm['name'] = itm.pop('PNPDeviceID')
    itm['NetConnectionStatus'] = \
        NET_CONN_STATUS.get(itm['NetConnectionStatus'], 'Other')
    itm['ConfigManagerErrorMsg'] = \
        CONFIG_MAN_ERR_CODE.get(itm['ConfigManagerErrorCode'])
    return itm


def on_item_route(itm: dict) -> dict:
    itm['name'] = '{Destination} [{InterfaceIndex}]'.format_map(itm)
    itm['Metric'] = itm.pop('Metric1')
    itm['Protocol'] = ROUTE_PROTOCOL_MAP.get(itm['Protocol'])
    itm['Type'] = ROUTE_TYPE_MAP.get(itm['Type'])
    return itm


async def check_network(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, ADAPTER_QUERY)
            state = get_state(ADAPTER_TYPE, rows, on_item_adapter)

            rows = await wmiquery(conn, service, INTERFACE_QUERY)
            state.update(get_state(INTERFACE_TYPE, rows))

            rows = await wmiquery(conn, service, ROUTE_QUERY)
            state.update(get_state(ROUTE_TYPE, rows, on_item_route))

            rows = await wmiquery(conn, service, INTERFACE_QUERY_RAW)
            rows_lk = {i['Name']: i for i in rows}
            prev = CACHE.get(asset.id)
            CACHE[asset.id] = rows_lk
            if prev:
                ct, _ = on_counters(rows_lk, prev, {
                    'BytesReceivedPersec': 'PERF_COUNTER_BULK_COUNT',
                    'BytesSentPersec': 'PERF_COUNTER_BULK_COUNT',
                })
                state[INTERFACE_TYPE_RAW] = [ct]
        finally:
            wmiclose(conn, service)
        return state
