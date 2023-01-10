import logging
from aiowmi.query import Query
from aiowmi.ndr.property_info import PropertyInfo
from libprobe.asset import Asset
from libprobe.exceptions import CheckException
from .asset_lock import get_asset_lock
from ..utils import get_state
from ..values import DRIVE_TYPES, AVAILABILITY_STATUS
from ..wmiquery import wmiconn, wmiquery, wmiclose


PHYSICAL_TYPE = "physical"
PHYSICAL_QUERY = Query("""
    SELECT
    Name, AvgDiskReadQueueLength, AvgDiskWriteQueueLength,
    DiskReadBytesPersec, DiskReadsPersec,
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
    Name, Automount, Availability, Capacity, Compressed, DeviceID, DirtyBitSet,
    DriveLetter, DriveType, FileSystem, FreeSpace, IndexingEnabled, Label,
    MaximumFileNameLength, QuotasEnabled, QuotasIncomplete, QuotasRebuilding,
    SerialNumber, SupportsDiskQuotas, SupportsFileBasedCompression
    FROM Win32_Volume  WHERE Name != "_Total" AND DriveType != 5
""")
SHADOW_TYPE = "shadow"
SHADOW_QUERY = Query("""
    SELECT
    Volume, MaxSpace, AllocatedSpace, UsedSpace
    FROM Win32_ShadowStorage
""")


def on_item_volume(itm: dict) -> dict:
    free = itm['FreeSpace']
    total = itm['Capacity']
    used = total - free
    pct = 100. * used / total if total else 0.

    itm['name'] = itm.pop('Name')
    itm['Availability'] = \
        AVAILABILITY_STATUS.get(itm['Availability'], 'Unknown')
    itm['DriveType'] = DRIVE_TYPES.get(itm['DriveType'], 'Unknown')
    itm['PercentUsed'] = pct
    return itm


async def volume_ref(
        conn: Connection,
        service: Service,
        row: dict):
    # Volume Name is expected to be unique and can therfore be used as
    # item name
    prop: PropertyInfo = row.pop('Volume')
    try:
        res = await prop.get_reference(conn, service, filter_props=['Name'])
        ref_props = res.get_properties()
        row['name'] = ref_props['Name'].value
    except Exception as e:
        error_msg = str(e) or type(e).__name__
        # At this point log the exception as this can be useful for debugging
        # issues with WMI queries;
        logging.exception(f'query error: {error_msg};')
        raise CheckException(error_msg)


async def check_storage(
        asset: Asset,
        asset_config: dict,
        check_config: dict) -> dict:
    async with get_asset_lock(asset):
        conn, service = await wmiconn(asset, asset_config, check_config)
        try:
            rows = await wmiquery(conn, service, PHYSICAL_QUERY)
            state = get_state(PHYSICAL_TYPE, rows)

            rows = await wmiquery(conn, service, LOGICAL_QUERY)
            state.update(get_state(LOGICAL_TYPE, rows))

            rows = await wmiquery(conn, service, VOLUME_QUERY)
            state.update(get_state(VOLUME_TYPE, rows, on_item_volume))

            rows = await wmiquery(conn, service, SHADOW_QUERY, keep_ref=True)
            for row in rows:
                await volume_ref(conn, service, row)
            state.update({SHADOW_TYPE: rows})
        finally:
            wmiclose(conn, service)

        return state
