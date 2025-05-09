import datetime
import time
from typing import Callable, Dict, List, Union


class PidLookup:
    _lk = {}
    _MAX_AGE = 900

    @classmethod
    def get(cls, asset_id: int) -> Union[Dict[int, str], None]:
        ts, data = cls._lk.get(asset_id, (None, None))
        if ts is None or time.time() - ts > cls._MAX_AGE:
            return
        return data

    @classmethod
    def set(cls, asset_id: int, rows: list) -> Dict[int, str]:
        data = {
            row['IDProcess']: row['Name'].split('#')[0]
            for row in rows
        }
        cls._lk[asset_id] = (time.time(), data)
        return data


def parse_wmi_date(val, fmt: str = '%Y%m%d') -> Union[int, None]:
    if not val:
        return None
    try:
        val = int(datetime.datetime.strptime(val, fmt).timestamp())
        if val <= 0:
            return None
        return val
    except Exception:
        return None


def parse_wmi_date_1600(val) -> Union[int, None]:
    if not val:
        return None
    seconds1600 = 11644473600  # seconds from 1600
    try:
        val = int(val, 16) // 10000000 - seconds1600
        if val <= 0:
            return None
        return val
    except Exception:
        return None


def get_item(row: dict, name: str = 'Name') -> dict:
    """This is the default get item function. It requires at least that Name
    is a key in the row data."""
    row['name'] = row.pop(name)
    return row


def add_total_item(state: dict, total_item: dict, type_name: str):
    """Add a new Type to the state with a single item: `total`"""
    total_item['name'] = 'total'
    state[f"{type_name}Total"] = [total_item]


def get_state_total(
        type_name: str,
        rows: List[dict],
        on_item: Callable[[dict], dict] = get_item) -> dict:
    """Default get_state function."""

    item_list = []
    state = {type_name: item_list}

    for row in rows:
        item = on_item(row)

        # For some queries a Name='_Total' item exists. In this case we want to
        # create a new type ending with Total;
        if item['name'] == '_Total':
            add_total_item(state, item, type_name)
        else:
            item_list.append(item)
    return state


def get_state(
        type_name: str,
        rows: List[dict],
        on_item: Callable[[dict], dict] = get_item) -> dict:
    """Default get_state function."""
    return {type_name: [on_item(itm) for itm in rows]}
