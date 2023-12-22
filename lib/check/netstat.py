from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..utils import PidLookup
from ..wmiquery import wmiconn, wmiquery, wmiclose

STATE_LK = {
    1: 'Closed',
    2: 'Listen',
    3: 'SynSent',
    4: 'SynReceived',
    5: 'Established',
    6: 'FinWait1',
    7: 'FinWait2',
    8: 'CloseWait',
    9: 'Closing',
    10: 'LastAck',
    11: 'TimeWait',
    12: 'DeleteTCB',
    100: 'Bound',  # no official documentation found
}

TYPE_NAME = "netstat"
QUERY = Query("""
    SELECT
    CreationTime, InstanceID, LocalAddress, LocalPort, OwningProcess,
    RemoteAddress, RemotePort, State
    FROM MSFT_NetTCPConnection
""", namespace=r'ROOT\StandardCIMV2')

PID_QUERY = Query("""
    SELECT
    Name, IDProcess
    FROM Win32_PerfFormattedData_PerfProc_Process WHERE Name != "_Total"
""")


async def check_netstat(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)

        try:
            rows = await wmiquery(conn, service, QUERY)

            # retrieve pid lookup,
            # when empty or aged query it here and set
            pid_lk = PidLookup.get(asset.id)
            if pid_lk is None:
                pid_rows = await wmiquery(conn, service, PID_QUERY)
                pid_lk = PidLookup.set(asset.id, pid_rows)
        finally:
            wmiclose(conn, service)

        for row in rows:
            row['name'] = row.pop('InstanceID')
            row['CreationTime'] = int(row['CreationTime'])  # float timestamp
            row['OwningProcessID'] = row['OwningProcess']
            row['OwningProcess'] = pid_lk.get(row['OwningProcess'])
            row['State'] = STATE_LK.get(row['State'])

        return {
            TYPE_NAME: rows
        }
