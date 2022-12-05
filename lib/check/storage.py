from aiowmi.query import Query
from libprobe.asset import Asset
from ..utils import get_state
from ..values import DRIVE_TYPES
from ..wmiquery import wmiconn, wmiquery, wmiclose


PHYSICAL_TYPE = "physical"
PHYSICAL_QUERY = Query("""
    SELECT
    Name, AvgDiskQueueLength, AvgDiskReadQueueLength, AvgDiskWriteQueueLength,
    CurrentDiskQueueLength, DiskReadBytesPersec, DiskReadsPersec,
    DiskWriteBytesPersec, DiskWritesPersec, PercentDiskReadTime,
    PercentDiskWriteTime
    FROM Win32_PerfFormattedData_PerfDisk_PhysicalDisk WHERE Name != "_Total"
""")
LOGICAL_TYPE = "logical"
LOGICAL_QUERY = Query("""
    SELECT
    Name, DiskReadsPersec, DiskWritesPersec
    FROM Win32_PerfFormattedData_PerfDisk_LogicalDisk
    WHERE Name != "_Total"
""")
VOLUME_TYPE = "volume"
VOLUME_QUERY = Query("""
    SELECT
    Name, Automount, Capacity, Compressed, DeviceID, DirtyBitSet, DriveLetter,
    DriveType, FileSystem, FreeSpace, IndexingEnabled, Label,
    MaximumFileNameLength, QuotasEnabled, QuotasIncomplete, QuotasRebuilding,
    SerialNumber, SupportsDiskQuotas, SupportsFileBasedCompression
    FROM Win32_Volume  WHERE Name != "_Total"
""")


def on_item_volume(itm: dict) -> dict:
    free = itm['FreeSpace']
    total = itm['Capacity']
    used = total - free
    pct = 100. * used / total if total else 0.

    itm['name'] = itm.pop('Name')
    itm['DriveType'] = DRIVE_TYPES.get(itm['DriveType'])
    itm['PercentUsed'] = pct
    return itm


async def check_storage(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    conn, service = await wmiconn(asset, asset_config, check_config)
    try:
        rows = await wmiquery(conn, service, PHYSICAL_QUERY)
        state = get_state(PHYSICAL_TYPE, rows)

        rows = await wmiquery(conn, service, LOGICAL_QUERY)
        state.update(get_state(LOGICAL_TYPE, rows))

        rows = await wmiquery(conn, service, VOLUME_QUERY)
        state.update(get_state(VOLUME_TYPE, rows, on_item_volume))
    finally:
        wmiclose(conn, service)

    return state
