import logging
from aiowmi.query import Query
from aiowmi.ndr.property_info import PropertyInfo
from aiowmi.connection import Connection
from aiowmi.connection import Protocol as Service
from libprobe.asset import Asset
from libprobe.severity import Severity
from libprobe.exceptions import CheckException, IncompleteResultException
from .asset_lock import get_asset_lock
from ..counters import on_counters
from ..utils import get_state
from ..values import DRIVE_TYPES
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
PHYSICAL_CACHE = {}
PHYSICAL_TYPE_RAW = "physicalCounters"
PHYSICAL_QUERY_RAW = Query("""
    SELECT
    Name, AvgDiskReadQueueLength, AvgDiskWriteQueueLength,
    DiskReadBytesPersec, DiskReadsPersec,
    DiskWriteBytesPersec, DiskWritesPersec, PercentDiskReadTime,
    PercentDiskWriteTime, Frequency_PerfTime, Timestamp_PerfTime
    FROM Win32_PerfRawData_PerfDisk_PhysicalDisk WHERE Name != "_Total"
""")
LOGICAL_TYPE = "logical"
LOGICAL_QUERY = Query("""
    SELECT
    Name, DiskReadsPersec, DiskWritesPersec
    FROM Win32_PerfFormattedData_PerfDisk_LogicalDisk
    WHERE Name != "_Total"
""")
LOGICAL_CACHE = {}
LOGICAL_TYPE_RAW = "logicalCounters"
LOGICAL_QUERY_RAW = Query("""
    SELECT
    Name, DiskReadsPersec, DiskWritesPersec
    FROM Win32_PerfRawData_PerfDisk_LogicalDisk
    WHERE Name != "_Total"
""")
VOLUME_TYPE = "volume"
VOLUME_QUERY = Query("""
    SELECT
    Name, Automount, Capacity, Compressed, DeviceID, DirtyBitSet,
    DriveLetter, DriveType, FileSystem, FreeSpace, IndexingEnabled, Label,
    MaximumFileNameLength, QuotasEnabled, QuotasIncomplete, QuotasRebuilding,
    SerialNumber, SupportsDiskQuotas, SupportsFileBasedCompression
    FROM Win32_Volume WHERE Name != "_Total" AND DriveType != 5
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
    itm['DriveType'] = DRIVE_TYPES.get(itm['DriveType'], 'Unknown')
    itm['PercentUsed'] = pct
    return itm


async def volume_ref(
        conn: Connection,
        service: Service,
        prop: PropertyInfo,
        row: dict):
    # Volume Name is expected to be unique and can therfore be used as
    # item name
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

            # At least one asset returns with a time-out on the shadow volume
            # query and therefore we ignore the type in the result. (issue #6)
            try:
                refs = {'Volume': volume_ref}
                rows = await wmiquery(
                    conn,
                    service,
                    SHADOW_QUERY,
                    refs=refs,
                    timeout=20)
                state.update({SHADOW_TYPE: rows})
            except (Exception, TimeoutError) as e:
                msg = str(e) or type(e).__name__
                raise IncompleteResultException(
                    f'failed to read shadow storage: {msg}',
                    result=state,
                    severity=Severity.LOW)

            rows = await wmiquery(conn, service, PHYSICAL_QUERY_RAW)
            rows_lk = {i['Name']: i for i in rows}
            prev = PHYSICAL_CACHE.get(asset.id)
            PHYSICAL_CACHE[asset.id] = rows_lk
            if prev:
                ct, _ = on_counters(rows_lk, prev, {
                    'AvgDiskReadQueueLength':
                        'PERF_COUNTER_100NS_QUEUELEN_TYPE',
                    'AvgDiskWriteQueueLength':
                        'PERF_COUNTER_100NS_QUEUELEN_TYPE',
                    'DiskReadBytesPersec': 'PERF_COUNTER_BULK_COUNT',
                    'DiskReadsPersec': 'PERF_COUNTER_COUNTER',
                    'DiskWriteBytesPersec': 'PERF_COUNTER_BULK_COUNT',
                    'DiskWritesPersec': 'PERF_COUNTER_COUNTER',
                    'PercentDiskReadTime': 'PERF_PRECISION_100NS_TIMER',
                })
                state[PHYSICAL_TYPE_RAW] = [ct]

            rows = await wmiquery(conn, service, LOGICAL_QUERY_RAW)
            rows_lk = {i['Name']: i for i in rows}
            prev = LOGICAL_CACHE.get(asset.id)
            LOGICAL_CACHE[asset.id] = rows_lk
            if prev:
                ct, _ = on_counters(rows_lk, prev, {
                    'DiskReadsPersec': 'PERF_COUNTER_COUNTER',
                    'DiskWritesPersec': 'PERF_COUNTER_COUNTER',
                })
                state[LOGICAL_TYPE_RAW] = [ct]

        finally:
            wmiclose(conn, service)

        return state
