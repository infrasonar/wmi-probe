import datetime
import time
from aiowmi.query import Query
from libprobe.asset import Asset
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..utils import get_state, get_state_total


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
    Name, PercentProcessorTime
    FROM Win32_PerfFormattedData_PerfOS_Processor
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


async def check_system(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, check_config)
    try:
        rows = await wmiquery(conn, service, SYSTEM_QUERY)
        state = get_state(SYSTEM_TYPE, rows, on_item)

        rows = await wmiquery(conn, service, TIME_QUERY)
        state.update(get_state(TIME_TYPE, rows, on_item_time))

        rows = await wmiquery(conn, service, OS_QUERY)
        state.update(get_state(OS_TYPE, rows, on_item_os))

        rows = await wmiquery(conn, service, PROCESSOR_QUERY)
        state.update(get_state_total(PROCESSOR_TYPE, rows))
    finally:
        wmiclose(conn, service)
    return state
