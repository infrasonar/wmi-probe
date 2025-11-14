from aiowmi.query import Query
from libprobe.asset import Asset
from libprobe.check import Check
from .asset_lock import get_asset_lock
from ..utils import get_state
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..values import SHARE_TYPE

DOMAIN_TYPE_NAME = "domain"
DOMAIN_QUERY = Query("""
    SELECT
    DomainName, DnsForestName, DomainControllerName
    FROM Win32_NTDomain
    WHERE DomainName IS NOT NULL
""")
SHARE_TYPE_NAME = "share"
SHARE_QUERY = Query("""
    SELECT
    AllowMaximum, Caption, Description, MaximumAllowed, Name, Path,
    Status, Type
    FROM Win32_Share
""")


def on_domain_item(itm: dict) -> dict:
    itm['DomainControllerName'] = itm['DomainControllerName'].strip('\\\\')
    itm['name'] = itm.pop('DomainName')
    return itm


def on_share_item(itm: dict) -> dict:
    itm['name'] = itm.pop('Name')
    itm['Type'] = SHARE_TYPE.get(itm['Type'], 'Unknown')
    return itm


class CheckNtDomain(Check):
    key = 'ntDomain'
    unchanged_eol: int = 14400

    @staticmethod
    async def run(asset: Asset, local_config: dict, config: dict) -> dict:
        async with get_asset_lock(asset):
            conn, service = await wmiconn(asset, local_config, config)
            try:
                rows = await wmiquery(conn, service, DOMAIN_QUERY)
                state = get_state(DOMAIN_TYPE_NAME, rows, on_domain_item)

                rows = await wmiquery(conn, service, SHARE_QUERY)
                state.update(get_state(SHARE_TYPE_NAME, rows, on_share_item))
            finally:
                wmiclose(conn, service)
            return state
