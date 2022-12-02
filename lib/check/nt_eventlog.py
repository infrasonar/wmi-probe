from aiowmi.query import Query
from libprobe.asset import Asset
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..utils import get_state


TYPE_NAME = "eventlog"
QUERY = Query("""
    SELECT
    FileName, Name, NumberOfRecords, Status
    FROM Win32_NTEventlogFile
""")


async def check_nt_eventlog(
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
