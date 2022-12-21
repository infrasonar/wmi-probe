import asyncio
from collections import defaultdict
from libprobe.asset import Asset

# Asset lock is used as we do not want one check to influence another check
# on the same asset. We do not clean the dict when assets are removed, thus
# this dict might contain locks for a few too much assets.
_asset_lock = defaultdict(asyncio.Lock)


def get_asset_lock(asset: Asset) -> asyncio.Lock:
    return _asset_lock[asset.id]
