from aiowmi.query import Query
import datetime
import time
from aiowmi.query import Query
from libprobe.asset import Asset
from typing import Tuple
from ..utils import get_state
from ..wmiquery import wmiquery


TYPE_NAME = "system"
QUERY = Query("""
    SELECT
    Year, Month, Day, Hour, Minute, Second
    FROM Win32_UTCTime
""")


def on_item(itm: dict) -> dict:
    remote_ts = datetime.datetime(
        itm['Year'], itm['Month'], itm['Day'], itm['Hour'],
        itm['Minute'], itm['Second'],
        tzinfo=datetime.timezone.utc
    ).timestamp()
    ts = time.time()
    diff = abs(remote_ts - ts)
    return {
        'name': TYPE_NAME,
        'TimeDifference': diff
    }


async def check_system_time(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    rows = await wmiquery(asset, asset_config, check_config, QUERY)
    state = get_state(TYPE_NAME, rows, on_item)
    return state