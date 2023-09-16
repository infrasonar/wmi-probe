import logging
from collections import defaultdict
from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..wmiquery import wmiconn, wmiquery, wmiclose


LOGGED_ON_TYPE = "loggedOn"
LOGGED_ON_QUERY = Query("""
    SELECT
    Antecedent
    FROM Win32_LoggedOnUser
""")
REMOTE_USERS_TYPE = "remote"
REMOTE_USERS_QUERY = Query("""
    SELECT
    Caption
    FROM Win32_Process
    WHERE Caption=\'winlogon.exe\'
""")


def get_itemname(itm):
    try:
        splitted = itm['Antecedent'].split('"')
        return splitted[1] + '\\' + splitted[3]
    except Exception as e:
        logging.error(e)
        return None


async def check_users(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, REMOTE_USERS_QUERY)
            state = {
                REMOTE_USERS_TYPE: [{
                    'name': REMOTE_USERS_TYPE,
                    'Count': len(rows) - 1,
                }]
            }

            rows = await wmiquery(conn, service, LOGGED_ON_QUERY)
            name_login = defaultdict(int)
            for itm in rows:
                name = get_itemname(itm)
                name_login[name] += 1

            state[LOGGED_ON_TYPE] = [{
                'name': name,
                'SessionCount': count
            } for name, count in name_login.items()]
            state[f'{LOGGED_ON_TYPE}Total'] = [{
                'name': 'total',
                'SessionCount': len(rows)
            }]
        finally:
            wmiclose(conn, service)
        return state
