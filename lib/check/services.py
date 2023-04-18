from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..utils import get_state


TYPE_NAME = "services"
QUERY = Query("""
    SELECT
    DesktopInteract, ExitCode, PathName, ServiceSpecificExitCode,
    ServiceType, State, Status, Name, DisplayName, StartMode, StartName,
    Started
    FROM Win32_Service
""")


async def check_services(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, QUERY)
            state = get_state(TYPE_NAME, rows)
        finally:
            wmiclose(conn, service)
        return state
