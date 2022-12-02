from aiowmi.query import Query
from libprobe.asset import Asset
from typing import Tuple
from ..utils import get_state
from ..wmiquery import wmiquery


TYPE_NAME = "operatingSystem"
QUERY = Query("""
    SELECT
    Caption, FreePhysicalMemory, TotalVisibleMemorySize
    FROM Win32_OperatingSystem
""")


def on_item(itm: dict) -> dict:
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


async def check_operating_system(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    rows = await wmiquery(asset, asset_config, check_config, QUERY)
    state = get_state(TYPE_NAME, rows, on_item)
    return state
