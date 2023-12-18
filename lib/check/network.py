import asyncio
from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..counters import perf_counter_counter
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
ROUTE_TYPE = "route"
ROUTE_QUERY = Query("""
    SELECT
    Destination, InterfaceIndex, Mask, Metric1, NextHop, Protocol, Type
    FROM Win32_IP4RouteTable
""")
TCPV4_CACHE = {}
TCPV4_TYPE = "tcpv4"
TCPV4_QUERY = Query("""
    SELECT
    ConnectionFailures, ConnectionsActive, ConnectionsEstablished, 
    ConnectionsPassive, ConnectionsReset, SegmentsPersec, 
    SegmentsReceivedPersec, SegmentsRetransmittedPersec, SegmentsSentPersec,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_TCPv4
""")
TCPV6_CACHE = {}
TCPV6_TYPE = "tcpv6"
TCPV6_QUERY = Query("""
    SELECT
    ConnectionFailures, ConnectionsActive, ConnectionsEstablished, 
    ConnectionsPassive, ConnectionsReset, SegmentsPersec, 
    SegmentsReceivedPersec, SegmentsRetransmittedPersec, SegmentsSentPersec,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_TCPv6
""")
UDPV4_CACHE = {}
UDPV4_TYPE = "udpv4"
UDPV4_QUERY = Query("""
    SELECT
    DatagramsNoPortPerSec, DatagramsPerSec, DatagramsReceivedErrors, 
    DatagramsReceivedPerSec, DatagramsSentPerSec,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_UDPv4
""")
UDPV6_CACHE = {}
UDPV6_TYPE = "udpv6"
UDPV6_QUERY = Query("""
    SELECT
    DatagramsNoPortPerSec, DatagramsPerSec, DatagramsReceivedErrors, 
    DatagramsReceivedPerSec, DatagramsSentPerSec, 
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_UDPv6
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

def validate(item, prev):
    d = item['Timestamp_PerfTime'] - prev['Timestamp_PerfTime']
    return d > 0


def on_tcp_item(name: str, item: dict, prev: dict):
    return {
        'name': name,
        'SegmentsPersec': perf_counter_counter(
            'SegmentsPersec', item, prev),
        'SegmentsReceivedPersec': perf_counter_counter(
            'SegmentsReceivedPersec', item, prev),
        'SegmentsRetransmittedPersec': perf_counter_counter(
            'SegmentsRetransmittedPersec', item, prev),
        'SegmentsSentPersec': perf_counter_counter(
            'SegmentsSentPersec', item, prev),
    }


def on_udp_item(name: str, item: dict, prev: dict):
    return {
        'name': name,
        'DatagramsNoPortPersec': perf_counter_counter(
            'DatagramsNoPortPersec', item, prev),
        'DatagramsPersec': perf_counter_counter(
            'DatagramsPersec', item, prev),
        'DatagramsReceivedErrors': perf_counter_counter(
            'DatagramsReceivedErrors', item, prev),
        'DatagramsReceivedPersec': perf_counter_counter(
            'DatagramsReceivedPersec', item, prev),
        'DatagramsSentPersec': perf_counter_counter(
            'DatagramsSentPersec', item, prev),
    }


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

            rows = await wmiquery(conn, service, TCPV4_QUERY)
            prev = TCPV4_CACHE.get(asset.id)
            while prev is None or not validate(rows[0], prev):
                prev = rows[0]
                await asyncio.sleep(3)
                rows = await wmiquery(conn, service, TCPV4_QUERY)
            TCPV4_CACHE[asset.id] = rows[0]
            state[TCPV4_TYPE] = [on_tcp_item(TCPV4_TYPE, rows[0], prev)]

            rows = await wmiquery(conn, service, TCPV6_QUERY)
            prev = TCPV6_CACHE.get(asset.id)
            while prev is None or not validate(rows[0], prev):
                prev = rows[0]
                await asyncio.sleep(3)
                rows = await wmiquery(conn, service, TCPV6_QUERY)
            TCPV6_CACHE[asset.id] = rows[0]
            state[TCPV6_TYPE] = [on_tcp_item(TCPV6_TYPE, rows[0], prev)]

            rows = await wmiquery(conn, service, UDPV4_QUERY)
            prev = UDPV4_CACHE.get(asset.id)
            while prev is None or not validate(rows[0], prev):
                prev = rows[0]
                await asyncio.sleep(3)
                rows = await wmiquery(conn, service, UDPV4_QUERY)
            UDPV4_CACHE[asset.id] = rows[0]
            state[UDPV4_TYPE] = [on_udp_item(UDPV4_TYPE, rows[0], prev)]
            
            rows = await wmiquery(conn, service, UDPV6_QUERY)
            prev = UDPV6_CACHE.get(asset.id)
            while prev is None or not validate(rows[0], prev):
                prev = rows[0]
                await asyncio.sleep(3)
                rows = await wmiquery(conn, service, UDPV6_QUERY)
            UDPV6_CACHE[asset.id] = rows[0]
            state[UDPV6_TYPE] = [on_udp_item(UDPV6_TYPE, rows[0], prev)]
            
        finally:
            wmiclose(conn, service)
        return state
