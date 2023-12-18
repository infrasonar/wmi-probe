from aiowmi.query import Query
from collections import defaultdict
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..utils import PidLookup
from ..wmiquery import wmiconn, wmiquery, wmiclose


TYPE_NAME = "process"
QUERY = Query("""
    SELECT
    Name, CreatingProcessID, ElapsedTime, HandleCount, IDProcess,
    PageFaultsPersec, PageFileBytes, PageFileBytesPeak, PercentPrivilegedTime,
    PercentProcessorTime, PercentUserTime, PoolNonpagedBytes, PoolPagedBytes,
    PriorityBase, PrivateBytes, ThreadCount, VirtualBytes, VirtualBytesPeak,
    WorkingSet, WorkingSetPeak
    FROM Win32_PerfFormattedData_PerfProc_Process WHERE Name != "_Total"
""")


def new_item():
    return {
        'CreatingProcessID': [],
        'ElapsedTime': 0,
        'HandleCount': 0,
        'IDProcess': [],
        'PageFaultsPersec': 0,
        'PageFileBytes': 0,
        'PageFileBytesPeak': 0,
        'PercentPrivilegedTime': 0,
        'PercentProcessorTime': 0,
        'PercentUserTime': 0,
        'PoolNonpagedBytes': 0,
        'PoolPagedBytes': 0,
        'PriorityBase': [],
        'PrivateBytes': 0,
        'ProcessCount': 0,
        'ThreadCount': 0,
        'VirtualBytes': 0,
        'VirtualBytesPeak': 0,
        'WorkingSet': 0,
        'WorkingSetPeak': 0,
    }


async def check_process(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, QUERY)
        finally:
            wmiclose(conn, service)
        
        # update the pid lookup which is used by netstat check
        PidLookup.set(rows)

        idict = defaultdict(new_item)
        for row in rows:
            name = row['Name'].split('#')[0]
            itm = idict[name]
            itm['name'] = name
            itm['CreatingProcessID'].append(row['CreatingProcessID'])
            itm['ElapsedTime'] += row['ElapsedTime']
            itm['HandleCount'] += row['HandleCount']
            itm['IDProcess'].append(row['IDProcess'])
            itm['PageFaultsPersec'] += row['PageFaultsPersec']
            itm['PageFileBytes'] += row['PageFileBytes']
            itm['PageFileBytesPeak'] = max(
                itm['PageFileBytesPeak'],
                row['PageFileBytesPeak'])
            itm['PercentPrivilegedTime'] += row['PercentPrivilegedTime']
            itm['PercentProcessorTime'] += row['PercentProcessorTime']
            itm['PercentUserTime'] += row['PercentUserTime']
            itm['PoolNonpagedBytes'] += row['PoolNonpagedBytes']
            itm['PoolPagedBytes'] += row['PoolPagedBytes']
            itm['PriorityBase'].append(row['PriorityBase'])
            itm['PrivateBytes'] += row['PrivateBytes']
            itm['ProcessCount'] += 1
            itm['ThreadCount'] += row['ThreadCount']
            itm['VirtualBytes'] += row['VirtualBytes']
            itm['VirtualBytesPeak'] = max(
                itm['VirtualBytesPeak'],
                row['VirtualBytesPeak'])
            itm['WorkingSet'] += row['WorkingSet']
            itm['WorkingSetPeak'] = max(
                itm['WorkingSetPeak'],
                row['WorkingSetPeak'])


        return {TYPE_NAME: list(idict.values())}
