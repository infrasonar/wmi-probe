from aiowmi.query import Query
from libprobe.asset import Asset
from typing import Tuple
from ..utils import get_state
from ..wmiquery import wmiquery


TYPE_NAME = "memory"
QUERY = Query("""
    SELECT
    CommitLimit, CommittedBytes, PercentCommittedBytesInUse
    FROM Win32_PerfFormattedData_PerfOS_Memory
""")


def on_item(itm: dict) -> dict:
    itm['name'] = TYPE_NAME
    return itm


async def check_memory(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    rows = await wmiquery(asset, asset_config, check_config, QUERY)
    state = get_state(TYPE_NAME, rows, on_item)
    return state