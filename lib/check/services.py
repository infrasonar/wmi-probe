from aiowmi.query import Query
from libprobe.asset import Asset
from libprobe.check import Check
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


class CheckServices(Check):
    key = 'services'
    unchanged_eol: int = 14400

    @staticmethod
    async def run(asset: Asset, local_config: dict, config: dict) -> dict:
        async with get_asset_lock(asset):
            conn, service = await wmiconn(asset, local_config, config)
            try:
                rows = await wmiquery(conn, service, QUERY)
                state = get_state(TYPE_NAME, rows)
            finally:
                wmiclose(conn, service)
            return state
