from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..utils import parse_wmi_date, PidLookup
from ..wmiquery import wmiconn, wmiquery, wmiclose

TYPE_NAME = "netstat"
QUERY = Query("""
    SELECT
    CreationTime, InstanceID, LocalAddress, LocalPort, OwningProcess, 
    RemoteAddress, RemotePort, RequestedState, State
    FROM MSFT_NetTCPConnection
""")

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

            # retrieve pid lookup, when empty query it here and set it once
            pid_lk = PidLookup.get()
            if pid_lk is None:
                pid_rows = await wmiquery(conn, service, PID_QUERY)
                pid_lk = {
                    row['IDProcess']: row['name'].split('#')[0]
                    for row in pid_rows
                }
                PidLookup.set(pid_lk)
        finally:
            wmiclose(conn, service)
        
        for row in rows:
            row['name'] = row.pop('InstanceID')
            row['CreationTime'] = parse_wmi_date(row['CreationTime'])
            row['OwningProcessID'] = row['OwningProcess']
            row['OwningProcess'] = pid_lk.get(row['OwningProcess'])
            
        return {
            TYPE_NAME: rows
        }
