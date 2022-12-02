from aiowmi.query import Query
from libprobe.asset import Asset
from ..wmiquery import wmiquery
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
    rows = await wmiquery(asset, asset_config, check_config, QUERY)
    state = get_state(TYPE_NAME, rows, on_item)
    return state
