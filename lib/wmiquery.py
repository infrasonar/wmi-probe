import datetime
import logging
import asyncio
import re
from aiowmi.query import Query
from libprobe.asset import Asset
from libprobe.exceptions import CheckException, IgnoreCheckException
from aiowmi.connection import Connection
from aiowmi.connection import Protocol as Service
from aiowmi.exceptions import (
    WbemExInvalidClass, WbemExInvalidNamespace, WbemExInitializationFailure)
from typing import List, Tuple, Optional
from . import DOCS_URL


DTYPS_NOT_NULL = {
    int: 0,
    bool: False,
    float: 0.,
    list: [],
}
QUERY_TIMEOUT = 120
QUERY_CLASS_PAT = re.compile(r'\s+from\s(\w+)\s?', re.IGNORECASE)


def get_class(query: str) -> str:
    o = QUERY_CLASS_PAT.search(query)
    return o.group(1) if o else 'unknown'


async def wmiconn(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> Tuple[Connection, Service]:
    address = check_config.get('address')
    if not address:
        address = asset.name
    username = asset_config.get('username')
    password = asset_config.get('password')
    if username is None or password is None:
        raise CheckException(
            'Missing credentials. Please refer to the following documentation'
            f' for detailed instructions: <{DOCS_URL}>'
        )

    if '\\' in username:
        # Replace double back-slash with single if required
        username = username.replace('\\\\', '\\')
        domain, username = username.split('\\')
    elif '@' in username:
        username, domain = username.split('@')
    else:
        domain = ''

    conn = Connection(address, username, password, domain)
    service = None

    try:
        await conn.connect()
    except Exception as e:
        error_msg = str(e) or type(e).__name__
        raise CheckException(f'unable to connect: {error_msg}')

    try:
        service = await conn.negotiate_ntlm()
    except Exception as e:
        conn.close()
        error_msg = str(e) or type(e).__name__
        raise CheckException(f'unable to authenticate: {error_msg}')

    return conn, service


async def wmiquery(
        conn: Connection,
        service: Service,
        query: Query,
        refs: Optional[dict] = None,
        timeout: int = QUERY_TIMEOUT,
        ignore: bool = False) -> List[dict]:
    rows = []

    try:
        async with query.context(conn, service, timeout=timeout) as qc:
            async for props in qc.results():  # type: ignore
                row = {}
                for name, prop in props.items():
                    if refs and name in refs and prop.is_reference():
                        await refs[name](conn, service, prop, row)
                    elif prop.value is None:
                        row[name] = DTYPS_NOT_NULL.get(prop.get_type())
                    elif isinstance(prop.value, datetime.datetime):
                        row[name] = prop.value.timestamp()
                    elif isinstance(prop.value, datetime.timedelta):
                        row[name] = prop.value.seconds
                    else:
                        row[name] = prop.value
                rows.append(row)
    except WbemExInvalidClass:
        msg = f'invalid class: {get_class(query.query)}'
        if ignore:
            raise IgnoreCheckException(msg)
        raise CheckException(msg)
    except WbemExInvalidNamespace:
        msg = f'invalid namespace: {query.namespace}'
        if ignore:
            raise IgnoreCheckException(msg)
        raise CheckException(msg)
    except asyncio.TimeoutError:
        raise CheckException('WMI query timed out')
    except WbemExInitializationFailure as e:
        error_msg = str(e) or type(e).__name__
        raise CheckException(error_msg)
    except Exception as e:
        error_msg = str(e) or type(e).__name__
        # At this point log the exception as this can be useful for debugging
        # issues with WMI queries;
        logging.exception(f'query error: {error_msg};')
        raise CheckException(error_msg)
    return rows


def wmiclose(conn: Connection, service: Service):
    service.close()
    conn.close()
