import logging
from aiowmi.query import Query
from libprobe.asset import Asset
from libprobe.check import Check
from libprobe.exceptions import IgnoreCheckException
from .asset_lock import get_asset_lock
from ..utils import get_state, parse_wmi_date
from ..wmiquery import wmiconn, wmiquery, wmiclose
from ..values import INSTALL_STATES_LU, LANGUAGE_NAMES

INSTALLED_TYPE_NAME = "installed"
INSTALLED_QUERY = Query("""
    SELECT
    Description, InstallDate, InstallDate2, InstallState, Name, PackageCode,
    Vendor, Version
    FROM Win32_Product
""")
FEATURE_TYPE_NAME = "feature"
FEATURE_QUERY = Query("""
    SELECT
    Name
    FROM Win32_ServerFeature
""")


def on_installed(itm: dict) -> dict:
    try:
        language = int(itm['Language'])
        language_name = LANGUAGE_NAMES.get(language, language)
    except Exception:
        language_name = None
    install_date = itm.pop('InstallDate2', None) or \
        parse_wmi_date(itm['InstallDate'])
    install_state_number = itm['InstallState']
    install_state = INSTALL_STATES_LU.get(
        install_state_number, install_state_number)

    itm['name'] = itm.pop('PackageCode')
    return {
        **itm,
        'InstallDate': install_date,
        'InstallState': install_state,
        'Language': language_name,
    }


def on_feature(itm: dict) -> dict:
    """Feature names might include non-ascii compatible characters.
    The best we can do is to replace those with a question mark (?) using
    the error 'replace' option."""
    itm['name'] = itm.pop('Name').encode('ascii', errors='replace').decode()
    return itm


class CheckSoftware(Check):
    key = 'software'
    unchanged_eol: int = 3600 * 12

    @staticmethod
    async def run(asset: Asset, local_config: dict, config: dict) -> dict:
        async with get_asset_lock(asset):
            conn, service = await wmiconn(asset, local_config, config)
            try:
                rows = await wmiquery(conn, service, INSTALLED_QUERY)
                state = get_state(INSTALLED_TYPE_NAME, rows, on_installed)

                try:
                    rows = await wmiquery(
                        conn,
                        service,
                        FEATURE_QUERY,
                        ignore=True)
                except IgnoreCheckException as e:
                    logging.debug(str(e))
                else:
                    if rows:
                        state.update(
                            get_state(FEATURE_TYPE_NAME, rows, on_feature))
            finally:
                wmiclose(conn, service)
            return state
