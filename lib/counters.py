from typing import Tuple


def perf_elapsed_time(name: str, itm: dict, prev_itm: dict) -> float:
    return (itm['Timestamp_Object'] - itm[name]) / itm['Frequency_Object']


def perf_100nsec_timer_inv(name: str, itm: dict, prev_itm: dict) -> int:
    n = itm[name] - prev_itm[name]
    d = itm['Timestamp_Sys100NS'] - prev_itm['Timestamp_Sys100NS']
    return round(100 * (1 - n / d))


def perf_100ns_queuelen_type(name: str, itm: dict, prev_itm: dict) -> int:
    time_base = itm['Frequency_PerfTime']
    n = itm[name] - prev_itm[name]
    d = itm['Timestamp_Sys100NS'] - prev_itm['Timestamp_Sys100NS']
    return round(n / (d / time_base))


def perf_counter_counter(name: str, itm: dict, prev_itm: dict) -> int:
    time_base = itm['Frequency_PerfTime']
    n = itm[name] - prev_itm[name]
    d = itm['Timestamp_PerfTime'] - prev_itm['Timestamp_PerfTime']
    return round(n / (d / time_base))


def perf_precision_100nsec_timer(name: str, itm: dict, prev_itm: dict) -> int:
    n = itm[name] - prev_itm[name]
    d = itm['Timestamp_Sys100NS'] - prev_itm['Timestamp_Sys100NS']
    return round(n / d)


OTHER_METRICS = (
    'Frequency_Object',
    'Frequency_PerfTime',
    'Frequency_Sys100NS',
    'Timestamp_Object',
    'Timestamp_PerfTime',
    'Timestamp_Sys100NS',
)


def on_counters(
        counters: dict,
        counters_previous: dict,
        counters_map: dict) -> Tuple[list, list]:

    out = []
    out_total = []
    for itm_name in set(counters) & set(counters_previous):
        itm = counters[itm_name]
        prev = counters_previous[itm_name]

        counter = {}
        for m, v in itm.items():
            fun = counters_map.get(m)
            if fun:
                counter[m] = fun(m, itm, prev)
            elif m not in OTHER_METRICS:
                counter[m] = v

        if itm_name == '_Total':
            counter['name'] = 'total'
            out_total.append(counter)
        else:
            counter['name'] = itm_name
            out.append(counter)

    return out, out_total
