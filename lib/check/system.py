from aiowmi.query import Query
from libprobe.asset import Asset
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..utils import get_state


TYPE_NAME = "system"
QUERY = Query("""
    SELECT
    Processes, SystemUpTime
    FROM Win32_PerfFormattedData_PerfOS_System
""")


def on_item(itm: dict) -> dict:
    return {
        'name': TYPE_NAME,
        **itm,
    }


async def check_system(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, check_config)
    try:
        rows = await wmiquery(conn, service, QUERY)
        state = get_state(TYPE_NAME, rows)
    finally:
        wmiclose(conn, service)
    return state
