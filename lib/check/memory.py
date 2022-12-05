from aiowmi.query import Query
from libprobe.asset import Asset
from ..utils import get_state
from ..wmiquery import wmiconn, wmiquery, wmiclose


TYPE_NAME = "memory"
QUERY = Query("""
    SELECT
    CommitLimit, CommittedBytes, PercentCommittedBytesInUse
    FROM Win32_PerfFormattedData_PerfOS_Memory
""")
PAGEFILE_TYPE = "pageFile"
PAGEFILE_QUERY = Query("""
    SELECT
    Name, AllocatedBaseSize, CurrentUsage
    FROM Win32_PageFileUsage
""")


def on_item(itm: dict) -> dict:
    itm['name'] = TYPE_NAME
    return itm


def on_item_pagefile(itm: dict) -> dict:
    total = itm['AllocatedBaseSize'] * 1024 * 1024
    used = itm['CurrentUsage'] * 1024 * 1024
    free = total - used
    percentage = 100 * used / total if total else 0.
    return {
        'name': itm['Name'],
        'BytesTotal': total,
        'BytesFree': free,
        'BytesUsed': used,
        'PercentUsed': percentage
    }


async def check_memory(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, check_config)
    try:
        rows = await wmiquery(conn, service, QUERY)
        state = get_state(TYPE_NAME, rows, on_item)

        rows = await wmiquery(conn, service, PAGEFILE_QUERY)
        state.update(get_state(PAGEFILE_TYPE, rows, on_item_pagefile))
    finally:
        wmiclose(conn, service)

    total = on_item_pagefile({
        'Name': 'total',
        'AllocatedBaseSize': sum(itm['AllocatedBaseSize'] for itm in rows),
        'CurrentUsage': sum(itm['CurrentUsage'] for itm in rows),
    })
    state[f"{PAGEFILE_TYPE}Total"] = [total]
    return state
