"""GA4 data fetching — shared by server.py."""

import concurrent.futures

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric,
    FilterExpression, Filter, RunReportRequest,
    FilterExpressionList,
)

PROPERTIES = {
    "srm": "257995281",
    "sk":  "258026800",
    "us":  "264964195",
}

LOGIN_STATUS_VALUE = "true"

CONTENT_URL_PATTERNS = [
    '/video/', '/foundations/', '/hnbk/', '/ency/', '/books/', '/cases/', '/skills/',
    '/book/', '/mono/', '/report/', '/chapter/', '/reference/',
    '/books-and-reference', '/methods-map', '/dict/', '/chpt/',
    '/referenceandbooks', '/business', '/videocollections',
    '/project-planner', '/which-stats-test',
]

EXTERNAL_DISCOVERY_CHANNELS = {
    "Organic Search", "Organic Social", "Referral", "Organic Video",
    "Organic Shopping", "Display", "Paid Search", "Paid Social",
    "Paid Video", "Paid Other", "Affiliates", "Audio", "Cross-network",
    "Organic AI", "AI Search",
}
DIRECT_CHANNELS  = {"Direct"}
EXCLUDED_CHANNELS = {"Unassigned"}


def _auth_filter():
    # Sessions where login_status = 'true' OR authentication_subscription starts with 'true'
    return FilterExpression(
        or_group=FilterExpressionList(expressions=[
            FilterExpression(filter=Filter(
                field_name="customEvent:login_status",
                string_filter=Filter.StringFilter(
                    value=LOGIN_STATUS_VALUE,
                    match_type=Filter.StringFilter.MatchType.EXACT,
                ),
            )),
            FilterExpression(filter=Filter(
                field_name="customEvent:authentication_subscription",
                string_filter=Filter.StringFilter(
                    value="true",
                    match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                ),
            )),
        ])
    )


def _search_filter():
    return FilterExpression(filter=Filter(
        field_name="eventName",
        string_filter=Filter.StringFilter(
            value="view_search_results",
            match_type=Filter.StringFilter.MatchType.EXACT,
        ),
    ))


def _us_search_filter():
    return FilterExpression(filter=Filter(
        field_name="eventName",
        string_filter=Filter.StringFilter(
            value="parasol_universal_search",
            match_type=Filter.StringFilter.MatchType.EXACT,
        ),
    ))


def _and(f1, f2):
    if f1 is None: return f2
    if f2 is None: return f1
    return FilterExpression(
        and_group=FilterExpressionList(expressions=[f1, f2])
    )


def _contains_or(field, patterns, case_sensitive=True):
    if not patterns:
        raise ValueError("_contains_or requires at least one pattern")
    exprs = [FilterExpression(filter=Filter(
        field_name=field,
        string_filter=Filter.StringFilter(
            value=p,
            match_type=Filter.StringFilter.MatchType.CONTAINS,
            case_sensitive=case_sensitive,
        ),
    )) for p in patterns]
    return exprs[0] if len(exprs) == 1 else FilterExpression(
        or_group=FilterExpressionList(expressions=exprs)
    )


def fetch_channel_data(client, property_id, start_date, end_date, auth_only=False,
                       auth_filter=None, base_filter=None):
    if auth_only:
        af = auth_filter if auth_filter is not None else _auth_filter()
    else:
        af = None
    resp = client.run_report(RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="sessionDefaultChannelGroup")],
        metrics=[Metric(name="sessions")],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimension_filter=_and(base_filter, af),
        limit=50,
    ))
    return [(r.dimension_values[0].value, int(r.metric_values[0].value))
            for r in resp.rows]


def fetch_search_data(client, property_id, start_date, end_date, auth_only=False,
                      srch_filter=None, content_patterns=None, auth_filter=None, base_filter=None):
    if srch_filter is None:
        srch_filter = _search_filter()
    if content_patterns is None:
        content_patterns = CONTENT_URL_PATTERNS

    base = dict(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )

    if auth_only:
        auth_f = auth_filter if auth_filter is not None else _auth_filter()
    else:
        auth_f = None
    auth_f = _and(base_filter, auth_f)
    srch_f = _and(auth_f, srch_filter)
    ref_f  = _and(auth_f, FilterExpression(filter=Filter(
        field_name="pageReferrer",
        string_filter=Filter.StringFilter(
            value="/search/results/",
            match_type=Filter.StringFilter.MatchType.CONTAINS,
            case_sensitive=False,
        ),
    )))

    def _run(metrics, dim_filter):
        return client.run_report(RunReportRequest(**base, metrics=metrics, dimension_filter=dim_filter))

    def _n(resp, idx=0):
        return int(resp.rows[0].metric_values[idx].value) if resp.rows else 0

    # All independent queries run concurrently; sessions+eventCount combined to save a round-trip.
    content_f = _contains_or("pagePath", content_patterns, case_sensitive=False) if content_patterns else None
    # ref_f scoped to content pages when patterns are available — avoids counting every page after search.
    ref_content_f = _and(ref_f, content_f) if content_f else ref_f
    with concurrent.futures.ThreadPoolExecutor() as ex:
        f_total = ex.submit(_run, [Metric(name="sessions")], auth_f)
        f_srch  = ex.submit(_run, [Metric(name="sessions"), Metric(name="eventCount")], srch_f)
        f_ref   = ex.submit(_run, [Metric(name="screenPageViews")], ref_content_f)
        if content_patterns:
            f_cnt = ex.submit(_run, [Metric(name="sessions")], _and(auth_f, content_f))
            f_sc  = ex.submit(_run, [Metric(name="sessions")], _and(srch_f, content_f))

    total         = _n(f_total.result())
    srch_r        = f_srch.result()
    searched      = _n(srch_r)
    search_events = _n(srch_r, 1)
    content_views_from_search = _n(f_ref.result())

    if content_patterns:
        sessions_with_content    = _n(f_cnt.result())
        searched_reached_content = _n(f_sc.result())
    else:
        sessions_with_content    = 0
        searched_reached_content = 0

    content_no_search = max(0, sessions_with_content - searched_reached_content)
    neither           = max(0, total - searched - content_no_search)

    return {
        "total":                     total,
        "searched":                  searched,
        "content_no_search":         content_no_search,
        "neither":                   neither,
        "searched_reached_content":  searched_reached_content,
        "searched_no_content":       max(0, searched - searched_reached_content),
        "search_events":             search_events,
        "content_views_from_search": content_views_from_search,
    }


def categorise_channels(rows):
    external = direct = other = unassigned = 0
    breakdown = []
    for channel, sessions in rows:
        if channel in EXCLUDED_CHANNELS:
            unassigned += sessions
            continue
        breakdown.append({"channel": channel, "sessions": sessions})
        if channel in EXTERNAL_DISCOVERY_CHANNELS:
            external += sessions
        elif channel in DIRECT_CHANNELS:
            direct += sessions
        else:
            other += sessions
    total = external + direct + other
    return {
        "external": external, "direct": direct, "other": other, "total": total,
        "unassigned": unassigned,
        "breakdown": sorted(breakdown, key=lambda x: -x["sessions"]),
    }


def merge_channel_rows(rows_a, rows_b):
    counts = {}
    for channel, sessions in rows_a + rows_b:
        counts[channel] = counts.get(channel, 0) + sessions
    return list(counts.items())


def _pct(n, d):
    return round(n / d * 100, 1) if d else 0


def fetch_all(client, start_date, end_date, auth_only=False):
    us_srch = _us_search_filter()

    # All six property×section fetches run concurrently; each fetch_search_data
    # also parallelises its internal queries, so wall time ≈ one GA4 round-trip.
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        f_srm_ch = ex.submit(fetch_channel_data, client, PROPERTIES["srm"], start_date, end_date, auth_only)
        f_sk_ch  = ex.submit(fetch_channel_data, client, PROPERTIES["sk"],  start_date, end_date, auth_only)
        f_us_ch  = ex.submit(fetch_channel_data, client, PROPERTIES["us"],  start_date, end_date,
                             auth_only=False, base_filter=us_srch)
        f_srm_s  = ex.submit(fetch_search_data, client, PROPERTIES["srm"], start_date, end_date, auth_only)
        f_sk_s   = ex.submit(fetch_search_data, client, PROPERTIES["sk"],  start_date, end_date, auth_only)
        f_us_s   = ex.submit(fetch_search_data, client, PROPERTIES["us"],  start_date, end_date,
                             auth_only=False, srch_filter=us_srch, content_patterns=[], base_filter=us_srch)

    srm_ch, sk_ch, us_ch = f_srm_ch.result(), f_sk_ch.result(), f_us_ch.result()
    srm_s,  sk_s,  us_s  = f_srm_s.result(),  f_sk_s.result(),  f_us_s.result()

    combined_ch = merge_channel_rows(srm_ch, sk_ch)

    def _sum(key):
        return srm_s[key] + sk_s[key]

    return {
        "srm": {"channels": categorise_channels(srm_ch), "search": srm_s},
        "sk":  {"channels": categorise_channels(sk_ch),  "search": sk_s},
        "us":  {"channels": categorise_channels(us_ch),  "search": us_s},
        "combined": {
            "channels": categorise_channels(combined_ch),
            "search": {
                "total":                     _sum("total"),
                "searched":                  _sum("searched"),
                "content_no_search":         _sum("content_no_search"),
                "neither":                   _sum("neither"),
                "searched_reached_content":  _sum("searched_reached_content"),
                "searched_no_content":       _sum("searched_no_content"),
                "search_events":             _sum("search_events"),
                "content_views_from_search": _sum("content_views_from_search"),
            },
        },
    }
