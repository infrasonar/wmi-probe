import asyncio
import logging
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
    AdapterType, AutoSense, ConfigManagerErrorCode, InterfaceIndex, MACAddress,
    Manufacturer, NetConnectionID, NetConnectionStatus, NetEnabled,
    PhysicalAdapter, PNPDeviceID, ProductName, ServiceName, Speed
    FROM Win32_NetworkAdapter
""")
ADAPTER_CONF_QUERY = Query("""
    SELECT
    ArpAlwaysSourceRoute, ArpUseEtherSNAP, Caption, DHCPEnabled,
    DHCPLeaseExpires, DHCPLeaseObtained, DHCPServer, DNSDomain,
    DNSDomainSuffixSearchOrder, DNSEnabledForWINSResolution, DNSHostName,
    DNSServerSearchOrder, DatabasePath, DeadGWDetectEnabled, DefaultIPGateway,
    DefaultTOS, DefaultTTL, Description, DomainDNSRegistrationEnabled,
    ForwardBufferMemory, FullDNSRegistrationEnabled, GatewayCostMetric,
    IGMPLevel, IPAddress, IPConnectionMetric, IPEnabled,
    IPFilterSecurityEnabled, IPPortSecurityEnabled, IPSecPermitIPProtocols,
    IPSecPermitTCPPorts, IPSecPermitUDPPorts, IPSubnet, IPUseZeroBroadcast,
    Index, InterfaceIndex, KeepAliveInterval, KeepAliveTime, MACAddress, MTU,
    NumForwardPackets, PMTUBHDetectEnabled, PMTUDiscoveryEnabled, ServiceName,
    SettingID, TcpMaxConnectRetransmissions, TcpMaxDataRetransmissions,
    TcpNumConnections, TcpUseRFC1122UrgentPointer, TcpWindowSize,
    TcpipNetbiosOptions, WINSEnableLMHostsLookup, WINSHostLookupFile,
    WINSPrimaryServer, WINSScopeID, WINSSecondaryServer
    FROM Win32_NetworkAdapterConfiguration
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
TCP_TYPE = 'tcp'
TCPV4_CACHE = {}
TCPV4_NAME = "TCPv4"
TCPV4_QUERY = Query("""
    SELECT
    ConnectionFailures, ConnectionsActive, ConnectionsEstablished,
    ConnectionsPassive, ConnectionsReset, SegmentsPersec,
    SegmentsReceivedPersec, SegmentsRetransmittedPersec, SegmentsSentPersec,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_TCPv4
""")
TCPV6_CACHE = {}
TCPV6_NAME = "TCPv6"
TCPV6_QUERY = Query("""
    SELECT
    ConnectionFailures, ConnectionsActive, ConnectionsEstablished,
    ConnectionsPassive, ConnectionsReset, SegmentsPersec,
    SegmentsReceivedPersec, SegmentsRetransmittedPersec, SegmentsSentPersec,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_TCPv6
""")
UDP_TYPE = 'udp'
UDPV4_CACHE = {}
UDPV4_NAME = "UDPv4"
UDPV4_QUERY = Query("""
    SELECT
    DatagramsNoPortPersec, DatagramsPersec, DatagramsReceivedErrors,
    DatagramsReceivedPersec, DatagramsSentPersec,
    Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_Tcpip_UDPv4
""")
UDPV6_CACHE = {}
UDPV6_NAME = "UDPv6"
UDPV6_QUERY = Query("""
    SELECT
    DatagramsNoPortPersec, DatagramsPersec, DatagramsReceivedErrors,
    DatagramsReceivedPersec, DatagramsSentPersec,
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


def validate_tcp_item(item, prev):
    return item['Timestamp_PerfTime'] > prev['Timestamp_PerfTime'] and \
        item['SegmentsPersec'] >= prev['SegmentsPersec'] and \
        item['SegmentsSentPersec'] >= prev['SegmentsSentPersec'] and \
        item['SegmentsReceivedPersec'] >= prev['SegmentsReceivedPersec'] and \
        item['SegmentsRetransmittedPersec'] >= \
        prev['SegmentsRetransmittedPersec']


def validate_udp_item(item, prev):
    return item['Timestamp_PerfTime'] > prev['Timestamp_PerfTime'] and \
        item['DatagramsNoPortPersec'] >= prev['DatagramsNoPortPersec'] and \
        item['DatagramsPersec'] >= prev['DatagramsPersec'] and \
        item['DatagramsSentPersec'] >= prev['DatagramsSentPersec'] and \
        item['DatagramsReceivedPersec'] >= prev['DatagramsReceivedPersec']


def on_tcp_item(name: str, item: dict, prev: dict):
    return {
        'name': name,
        'ConnectionFailures': item['ConnectionFailures'],
        'ConnectionsActive': item['ConnectionsActive'],
        'ConnectionsEstablished': item['ConnectionsEstablished'],
        'ConnectionsPassive': item['ConnectionsPassive'],
        'ConnectionsReset': item['ConnectionsReset'],
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
        'DatagramsReceivedErrors': item['DatagramsReceivedErrors'],
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
            rows = await wmiquery(conn, service, ADAPTER_CONF_QUERY)
            adapter_conf_lookup = {
                row['InterfaceIndex']: row for row in rows
            }

            rows = await wmiquery(conn, service, ADAPTER_QUERY)
            adapter_if_lookup = {
                row['InterfaceIndex']: row['PNPDeviceID'] for row in rows
            }

            # merge adapter and adapter_configuration
            for row in rows:
                conf = adapter_conf_lookup.get(row['InterfaceIndex'])
                # TODO raise IncompleteException / all metrics optional?
                if conf is not None:
                    row.update(conf)
            state = get_state(ADAPTER_TYPE, rows, on_item_adapter)

            rows = await wmiquery(conn, service, INTERFACE_QUERY)
            state.update(get_state(INTERFACE_TYPE, rows))

            rows = await wmiquery(conn, service, ROUTE_QUERY)

            # add AdapterRef metric
            for row in rows:
                ref = adapter_if_lookup.get(row['InterfaceIndex'])
                row['AdapterRef'] = ref  # can be None
            state.update(get_state(ROUTE_TYPE, rows, on_item_route))

            tcp = []
            try:
                rows = await wmiquery(conn, service, TCPV4_QUERY)
                prev = TCPV4_CACHE.get(asset.id)
                while prev is None or not validate_tcp_item(rows[0], prev):
                    prev = rows[0]
                    await asyncio.sleep(3)
                    rows = await wmiquery(conn, service, TCPV4_QUERY)
                TCPV4_CACHE[asset.id] = rows[0]
                tcp.append(on_tcp_item(TCPV4_NAME, rows[0], prev))
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'failed TCPv4 query: {msg}')

            try:
                rows = await wmiquery(conn, service, TCPV6_QUERY)
                prev = TCPV6_CACHE.get(asset.id)
                while prev is None or not validate_tcp_item(rows[0], prev):
                    prev = rows[0]
                    await asyncio.sleep(3)
                    rows = await wmiquery(conn, service, TCPV6_QUERY)
                TCPV6_CACHE[asset.id] = rows[0]
                tcp.append(on_tcp_item(TCPV6_NAME, rows[0], prev))
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'failed TCPv6 query: {msg}')

            if tcp:
                state[TCP_TYPE] = tcp

            udp = []
            try:
                rows = await wmiquery(conn, service, UDPV4_QUERY)
                prev = UDPV4_CACHE.get(asset.id)
                while prev is None or not validate_udp_item(rows[0], prev):
                    prev = rows[0]
                    await asyncio.sleep(3)
                    rows = await wmiquery(conn, service, UDPV4_QUERY)
                UDPV4_CACHE[asset.id] = rows[0]
                udp.append(on_udp_item(UDPV4_NAME, rows[0], prev))
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'failed UDPv4 query: {msg}')

            try:
                rows = await wmiquery(conn, service, UDPV6_QUERY)
                prev = UDPV6_CACHE.get(asset.id)
                while prev is None or not validate_udp_item(rows[0], prev):
                    prev = rows[0]
                    await asyncio.sleep(3)
                    rows = await wmiquery(conn, service, UDPV6_QUERY)
                UDPV6_CACHE[asset.id] = rows[0]
                udp.append(on_udp_item(UDPV6_NAME, rows[0], prev))
            except Exception as e:
                msg = str(e) or type(e).__name__
                logging.error(f'failed UDPv6 query: {msg}')

            if udp:
                state[UDP_TYPE] = udp

        finally:
            wmiclose(conn, service)
        return state
