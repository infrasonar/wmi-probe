import asyncio
import datetime
import time
from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..counters import on_counters, perf_100nsec_timer_inv
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..utils import get_state

_CACHE = {}

SYSTEM_TYPE = "system"
SYSTEM_QUERY = Query("""
    SELECT
    Processes, SystemUpTime
    FROM Win32_PerfFormattedData_PerfOS_System
""")
TIME_TYPE = "time"
TIME_QUERY = Query("""
    SELECT
    Year, Month, Day, Hour, Minute, Second
    FROM Win32_UTCTime
""")
OS_TYPE = "os"
OS_QUERY = Query("""
    SELECT
    Caption, FreePhysicalMemory, TotalVisibleMemorySize
    FROM Win32_OperatingSystem
""")
PROCESSOR_TYPE = "processor"
PROCESSOR_QUERY = Query("""
    SELECT
    Name, Timestamp_Sys100NS, PercentProcessorTime
    FROM Win32_PerfRawData_PerfOS_Processor
""")


def on_item(itm: dict) -> dict:
    return {
        'name': SYSTEM_TYPE,
        **itm,
    }


def on_item_time(itm: dict) -> dict:
    remote_ts = datetime.datetime(
        itm['Year'], itm['Month'], itm['Day'], itm['Hour'],
        itm['Minute'], itm['Second'],
        tzinfo=datetime.timezone.utc
    ).timestamp()
    ts = time.time()
    diff = abs(remote_ts - ts)
    return {
        'name': TIME_TYPE,
        'TimeDifference': diff
    }


def on_item_os(itm: dict) -> dict:
    free = itm['FreePhysicalMemory']
    total = itm['TotalVisibleMemorySize']
    used = total - free
    pct = 100. * used / total if total else 0.

    return {
        'name': itm['Caption'].strip(),
        'TotalVisibleMemorySize': total,
        'FreePhysicalMemory': free,
        'MemoryUsed': used,
        'MemoryUsedPercent': pct,
    }


def validate(item, prev):
    n = item['PercentProcessorTime'] - prev['PercentProcessorTime']
    d = item['Timestamp_Sys100NS'] - prev['Timestamp_Sys100NS']
    return n >= 0 and d >= 0 and n > d


async def check_system(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, SYSTEM_QUERY)
            state = get_state(SYSTEM_TYPE, rows, on_item)

            rows = await wmiquery(conn, service, TIME_QUERY)
            state.update(get_state(TIME_TYPE, rows, on_item_time))

            rows = await wmiquery(conn, service, OS_QUERY)
            state.update(get_state(OS_TYPE, rows, on_item_os))

            rows = await wmiquery(conn, service, PROCESSOR_QUERY)
            rows_lk = {i['Name']: i for i in rows}
            prev = _CACHE.get(asset.id)
            while prev is None or any(
                name not in prev or not validate(i, prev[name])
                for name, i in rows_lk.items()
            ):
                prev = rows_lk
                await asyncio.sleep(3)
                rows = await wmiquery(conn, service, PROCESSOR_QUERY)
                rows_lk = {i['Name']: i for i in rows}

            _CACHE[asset.id] = rows_lk
            ct, ct_total = on_counters(rows_lk, prev, {
                'PercentProcessorTime': perf_100nsec_timer_inv,
            })
            state[PROCESSOR_TYPE] = ct
            state[f'{PROCESSOR_TYPE}Total'] = ct_total
        finally:
            wmiclose(conn, service)
        return state
