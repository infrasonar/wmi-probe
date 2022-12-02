from aiowmi.query import Query
from libprobe.asset import Asset
from ..utils import get_state
from ..wmiquery import wmiconn, wmiquery, wmiclose


PHYSICAL_TYPE = "physical"
PHYSICAL_QUERY = Query("""
    SELECT
    Name, AvgDiskQueueLength, AvgDiskReadQueueLength, AvgDiskWriteQueueLength,
    CurrentDiskQueueLength, DiskReadBytesPersec, DiskReadsPersec,
    DiskWriteBytesPersec, DiskWritesPersec, PercentDiskReadTime,
    PercentDiskWriteTime
    FROM Win32_PerfFormattedData_PerfDisk_PhysicalDisk
""")

LOGICAL_TYPE = "logical"
LOGICAL_QUERY = Query("""
    SELECT
    Name, DiskReadsPersec, DiskWritesPersec
    FROM Win32_PerfFormattedData_PerfDisk_LogicalDisk
    WHERE name != "_Total"
""")


async def check_disk(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, check_config)
    try:
        rows = await wmiquery(conn, service, PHYSICAL_QUERY)
        state = get_state(PHYSICAL_TYPE, rows)

        rows = await wmiquery(conn, service, LOGICAL_QUERY)
        state.update(get_state(LOGICAL_TYPE, rows))
    finally:
        wmiclose(conn, service)

    return state
