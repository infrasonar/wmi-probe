import logging
from collections import defaultdict
from aiowmi.query import Query
from libprobe.asset import Asset
from ..wmiquery import wmiconn, wmiquery, wmiclose


LOGGED_ON_TYPE = "loggedOn"
LOGGED_ON_QUERY = Query("""
    SELECT
    Antecedent, Dependent
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


def get_logonid(itm):
    try:
        splitted = itm['Dependent'].split('"')
        return splitted[1]
    except Exception as e:
        logging.error(e)
        return None


async def check_remote_users(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
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
        name_login = defaultdict(list)
        for itm in rows:
            name = get_itemname(itm)
            logon_id = get_logonid(itm)
            name_login[name].append(logon_id)

        state[LOGGED_ON_TYPE] = [{
            'name': name,
            'LogonIds': logon_ids,
            'SessionCount': len(logon_ids)
        } for name, logon_ids in name_login.items()]

        state[f'{LOGGED_ON_TYPE}Count'] = [{
            'name': LOGGED_ON_TYPE,
            'Count': len(rows),
        }]
    finally:
        wmiclose(conn, service)
    return state
