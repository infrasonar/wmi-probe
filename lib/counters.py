def perf_elapsed_time(name: str, itm: dict, prev_itm: dict) -> dict:
    return (itm['Timestamp_Object'] - itm[name]) / itm['Frequency_Object']


def perf_100nsec_timer_inv(name: str, itm: dict, prev_itm: dict) -> dict:
    dx = itm[name] - prev_itm[name]
    dy = itm['Timestamp_Sys100NS'] - prev_itm['Timestamp_Sys100NS']
    return int(100 * (1 - dx / dy))


def perf_100ns_queuelen_type(name: str, itm: dict, prev_itm: dict) -> dict:
    time_base = itm['Frequency_PerfTime']
    dx = itm[name] - prev_itm[name]
    dy = itm['Timestamp_Sys100NS'] - prev_itm['Timestamp_Sys100NS']
    return int(dx / (dy / time_base))


def perf_counter_counter(name: str, itm: dict, prev_itm: dict) -> dict:
    time_base = itm['Frequency_PerfTime']
    dx = itm[name] - prev_itm[name]
    dy = itm['Timestamp_PerfTime'] - prev_itm['Timestamp_PerfTime']
    return int(dx / (dy / time_base))


def perf_precision_100nsec_timer(name: str, itm: dict, prev_itm: dict) -> dict:
    dx = itm[name] - prev_itm[name]
    dy = itm['Timestamp_Sys100NS'] - prev_itm['Timestamp_Sys100NS']
    return int(dx / dy)


COUNTER_FUNS = {
    'PERF_100NSEC_TIMER_INV': perf_100nsec_timer_inv,
    'PERF_COUNTER_100NS_QUEUELEN_TYPE': perf_100ns_queuelen_type,
    'PERF_COUNTER_BULK_COUNT': perf_counter_counter,
    'PERF_COUNTER_COUNTER': perf_counter_counter,
    'PERF_ELAPSED_TIME': perf_elapsed_time,
    'PERF_PRECISION_100NS_TIMER': perf_precision_100nsec_timer,
}

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
        counters_info: dict) -> list:

    out = []
    out_total = []
    for itm_name in set(counters) & set(counters_previous):
        itm = counters[itm_name]
        prev = counters_previous[itm_name]

        counter = {'name': itm_name}
        for m, v in itm.items():
            counter_name = counters_info.get(m)
            if counter_name:
                fun = COUNTER_FUNS[counter_name]
                counter[m] = fun(m, itm, prev)
            elif m not in OTHER_METRICS:
                counter[m] = v

        if itm_name == '_Total':
            out_total.append(counter)
        else:
            out.append(counter)

    return out, out_total
