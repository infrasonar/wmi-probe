from typing import List
from aiowmi.query import Query
from libprobe.asset import Asset
from .asset_lock import get_asset_lock
from ..utils import get_state
from ..wmiquery import wmiconn, wmiquery, wmiclose


SCHEDULE_JOBS_TYPE = "scheduledJobs"
SCHEDULE_JOBS_QUERY = Query("""
    SELECT
    Caption, Description, InstallDate, Name, Status, ElapsedTime, Priority,
    TimeSubmitted, UntilTime, DaysOfMonth, DaysOfWeek, InteractWithDesktop,
    JobId, JobStatus, RunRepeatedly, StartTime
    FROM Win32_ScheduledJob
""")


#   string   Caption;
#   string   Description;
#   datetime InstallDate;           -> int
#   string   Name;                  -> name
#   string   Status;
#   datetime ElapsedTime;           -> int
#   string   Notify;                -> skip
#   string   Owner;                 -> skip
#   uint32   Priority;
#   datetime TimeSubmitted;         -> int
#   datetime UntilTime;             -> int
#   string   Command;               -> skip
#   uint32   DaysOfMonth;           -> to list int
#   uint32   DaysOfWeek;            -> to list string
#   boolean  InteractWithDesktop;
#   uint32   JobId;
#   string   JobStatus;
#   boolean  RunRepeatedly;
#   datetime StartTime;             -> int


def get_days_of_month(inp: int) -> List[int]:
    return [i for i in range(1, 32) if i & inp]


def get_days_of_week(inp: int) -> List[str]:
    return [name for i, name in enumerate((
            'Monday',
            'Tuesday',
            'Wednesday',
            'Thursday',
            'Friday',
            'Saturday',
            'Sunday')) if (i+1) & inp]


def on_scheduled_job(itm: dict) -> dict:
    itm['name'] = itm.pop('Name').encode('ascii', errors='replace').decode()

    itm['DaysOfMonth'] = get_days_of_month(itm['DaysOfMonth'])
    itm['DaysOfWeek'] = get_days_of_week(itm['DaysOfWeek'])

    itm['InstallDate'] = int(itm['InstallDate'])
    itm['ElapsedTime'] = int(itm['ElapsedTime'])
    itm['TimeSubmitted'] = int(itm['TimeSubmitted'])
    itm['UntilTime'] = int(itm['UntilTime'])
    itm['StartTime'] = int(itm['StartTime'])

    return itm


async def check_scheduled_jobs(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, SCHEDULE_JOBS_QUERY)
            state = get_state(SCHEDULE_JOBS_TYPE, rows, on_scheduled_job)
        finally:
            wmiclose(conn, service)
        return state
