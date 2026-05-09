#!/usr/bin/env python3
"""
Generate Kibana saved_objects.ndjson.

Two fully independent dashboards:
  - UEFA Euro 2024 Analytics
  - La Liga 2015/16 Analytics

No data is mixed between competitions.
"""
import json

INDEX   = "football_events"
REPLAY  = "football_replay"
EP_ID   = "football-events-pattern"
RP_ID   = "football-replay-pattern"

objects = []


# ── helpers ────────────────────────────────────────────────────────────────

def _vis(vid, title, vis_state, search_json=None, refs=None):
    if search_json is None:
        search_json = {"query": {"query": "", "language": "kuery"}, "filter": []}
    return {
        "type": "visualization", "id": vid,
        "attributes": {
            "title": title,
            "visState": json.dumps(vis_state),
            "uiStateJSON": "{}",
            "description": "", "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(search_json)
            },
        },
        "references": refs or [],
        "migrationVersion": {},
        "updated_at": "2024-01-01T00:00:00.000Z",
        "version": "1",
    }


def metric_vis(vid, title, agg_type, agg_params, kuery="", subtitle="",
               index_id=EP_ID):
    vs = {
        "title": title, "type": "metric",
        "aggs": [{"id": "1", "enabled": True, "type": agg_type,
                  "schema": "metric", "params": agg_params}],
        "params": {
            "addTooltip": True, "addLegend": False, "type": "metric",
            "metric": {
                "percentageMode": False, "useRanges": False,
                "colorSchema": "Green to Red", "metricColorMode": "None",
                "colorsRange": [{"from": 0, "to": 10_000_000}],
                "labels": {"show": True}, "invertColors": False,
                "style": {
                    "bgFill": "#000", "bgColor": False, "labelColor": False,
                    "subText": subtitle, "fontSize": 44,
                },
            },
        },
    }
    sj = {
        "query": {"query": kuery, "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    refs = [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
             "type": "index-pattern", "id": index_id}]
    return _vis(vid, title, vs, sj, refs)


def table_vis(vid, title, aggs, kuery="", index_id=EP_ID):
    vs = {
        "title": title, "type": "table",
        "aggs": aggs,
        "params": {
            "type": "table", "perPage": 20,
            "showPartialRows": False, "showMetricsAtAllLevels": False,
            "sort": {"columnIndex": None, "direction": None},
            "showTotal": False, "totalFunc": "sum", "percentageCol": "",
        },
    }
    sj = {
        "query": {"query": kuery, "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    refs = [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
             "type": "index-pattern", "id": index_id}]
    return _vis(vid, title, vs, sj, refs)


def vega_vis(vid, title, spec):
    vs = {"title": title, "type": "vega", "aggs": [],
          "params": {"spec": json.dumps(spec)}}
    return _vis(vid, title, vs)


def comp_filter(comp_name):
    """ES term filter for a competition."""
    return {"term": {"competition_name": comp_name}}


def bool_must(*filters):
    return {"bool": {"must": list(filters)}}


# ── index patterns ─────────────────────────────────────────────────────────

objects.append({
    "type": "index-pattern", "id": EP_ID,
    "attributes": {"title": INDEX, "timeFieldName": "", "fields": "[]"},
    "references": [],
    "migrationVersion": {"index-pattern": "7.6.0"},
    "updated_at": "2024-01-01T00:00:00.000Z", "version": "1",
})
objects.append({
    "type": "index-pattern", "id": RP_ID,
    "attributes": {"title": REPLAY, "timeFieldName": "replay_ts",
                   "fields": "[]"},
    "references": [],
    "migrationVersion": {"index-pattern": "7.6.0"},
    "updated_at": "2024-01-01T00:00:00.000Z", "version": "1",
})


# ── per-competition visualisation factory ──────────────────────────────────

def make_competition_visuals(comp_name, label, prefix, color_scheme):
    """
    Generate ~12 visualisation objects for one competition.
    comp_name   – exact string stored in ES  e.g. "UEFA Euro"
    label       – display label              e.g. "UEFA Euro 2024"
    prefix      – id prefix                  e.g. "euro"
    color_scheme – Vega-Lite scheme name     e.g. "blues"
    """
    vises = []
    cf    = comp_filter(comp_name)           # ES term filter
    kq    = f'competition_name : "{comp_name}"'  # kuery filter

    # ── KPI strip ─────────────────────────────────────────────────────────

    vises.append(metric_vis(
        f"{prefix}-kpi-matches", f"{label} — Matches",
        "cardinality", {"field": "match_id"},
        kuery=kq, subtitle="Matches Played"))

    vises.append(metric_vis(
        f"{prefix}-kpi-goals", f"{label} — Goals",
        "count", {},
        kuery=f'{kq} AND shot_outcome : "Goal"', subtitle="Goals Scored"))

    vises.append(metric_vis(
        f"{prefix}-kpi-shots", f"{label} — Shots",
        "count", {},
        kuery=f'{kq} AND type_name : "Shot"', subtitle="Shot Attempts"))

    vises.append(metric_vis(
        f"{prefix}-kpi-xg", f"{label} — Total xG",
        "sum", {"field": "shot_xg"},
        kuery=kq, subtitle="Expected Goals"))

    vises.append(metric_vis(
        f"{prefix}-kpi-passes", f"{label} — Passes",
        "count", {},
        kuery=f'{kq} AND type_name : "Pass"', subtitle="Pass Events"))

    # ── Top 15 goalscorers ────────────────────────────────────────────────

    vises.append(vega_vis(
        f"{prefix}-top-scorers", f"{label} — Top 15 Goalscorers", {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {
            "url": {
                "index": INDEX,
                "body": {
                    "size": 0,
                    "query": bool_must(cf, {"term": {"shot_outcome": "Goal"}}),
                    "aggs": {
                        "by_player": {
                            "terms": {
                                "field": "player_name.keyword",
                                "size": 15,
                                "order": {"_count": "desc"},
                            },
                            "aggs": {
                                "top_team": {
                                    "terms": {"field": "team_name", "size": 1}
                                }
                            },
                        }
                    },
                },
            },
            "format": {"property": "aggregations.by_player.buckets"},
        },
        "transform": [
            {"calculate": "datum.key", "as": "player"},
            {"calculate": "datum.doc_count", "as": "goals"},
            {
                "calculate":
                    "datum.top_team.buckets.length > 0"
                    " ? datum.top_team.buckets[0].key : 'Unknown'",
                "as": "team",
            },
        ],
        "mark": {"type": "bar", "cornerRadiusEnd": 3},
        "encoding": {
            "y": {
                "field": "player", "type": "nominal", "sort": "-x",
                "title": None, "axis": {"labelLimit": 220},
            },
            "x": {"field": "goals", "type": "quantitative", "title": "Goals"},
            "color": {
                "field": "team", "type": "nominal", "title": "Team",
                "scale": {"scheme": "tableau20"},
            },
            "tooltip": [
                {"field": "player", "title": "Player"},
                {"field": "team",   "title": "Team"},
                {"field": "goals",  "title": "Goals"},
            ],
        },
        "width": 420, "height": 380, "autosize": "none",
    }))

    # ── Top teams by xG ───────────────────────────────────────────────────

    vises.append(vega_vis(
        f"{prefix}-top-teams-xg", f"{label} — Top Teams by xG", {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {
            "url": {
                "index": INDEX,
                "body": {
                    "size": 0,
                    "query": bool_must(cf, {"term": {"type_name": "Shot"}}),
                    "aggs": {
                        "by_team": {
                            "terms": {
                                "field": "team_name",
                                "size": 12,
                                "order": {"total_xg": "desc"},
                            },
                            "aggs": {
                                "total_xg": {"sum": {"field": "shot_xg"}},
                                "goals": {
                                    "filter": {
                                        "term": {"shot_outcome": "Goal"}
                                    }
                                },
                                "shots": {
                                    "value_count": {"field": "event_id"}
                                },
                            },
                        }
                    },
                },
            },
            "format": {"property": "aggregations.by_team.buckets"},
        },
        "transform": [
            {"calculate": "datum.key", "as": "team"},
            {"calculate": "datum.total_xg.value", "as": "xg"},
            {"calculate": "datum.goals.doc_count", "as": "goals"},
            {"calculate": "datum.shots.value", "as": "shots"},
        ],
        "mark": {"type": "bar", "cornerRadiusEnd": 3},
        "encoding": {
            "y": {
                "field": "team", "type": "nominal", "sort": "-x",
                "title": None, "axis": {"labelLimit": 200},
            },
            "x": {
                "field": "xg", "type": "quantitative",
                "title": "Total xG",
            },
            "color": {
                "field": "xg", "type": "quantitative",
                "scale": {"scheme": color_scheme},
                "legend": {"title": "xG"},
            },
            "tooltip": [
                {"field": "team",  "title": "Team"},
                {"field": "xg",    "title": "Total xG", "format": ".2f"},
                {"field": "goals", "title": "Goals"},
                {"field": "shots", "title": "Shots"},
            ],
        },
        "width": 360, "height": 300, "autosize": "none",
    }))

    # ── xG vs Goals scatter ───────────────────────────────────────────────

    vises.append(vega_vis(
        f"{prefix}-xg-scatter", f"{label} — xG vs Goals per Player", {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "layer": [
            {
                "data": {"values": [{"rx": 0, "ry": 0}, {"rx": 7, "ry": 7}]},
                "mark": {
                    "type": "line", "color": "#888888",
                    "strokeDash": [6, 3], "strokeWidth": 1.5,
                },
                "encoding": {
                    "x": {"field": "rx", "type": "quantitative",
                          "title": "Total xG"},
                    "y": {"field": "ry", "type": "quantitative",
                          "title": "Goals Scored"},
                },
            },
            {
                "data": {
                    "url": {
                        "index": INDEX,
                        "body": {
                            "size": 0,
                            "query": bool_must(
                                cf, {"term": {"type_name": "Shot"}}
                            ),
                            "aggs": {
                                "by_player": {
                                    "terms": {
                                        "field": "player_name.keyword",
                                        "size": 80,
                                        "order": {"total_xg": "desc"},
                                    },
                                    "aggs": {
                                        "total_xg": {
                                            "sum": {"field": "shot_xg"}
                                        },
                                        "goals_filter": {
                                            "filter": {
                                                "term": {
                                                    "shot_outcome": "Goal"
                                                }
                                            }
                                        },
                                        "top_team": {
                                            "terms": {
                                                "field": "team_name",
                                                "size": 1,
                                            }
                                        },
                                    },
                                }
                            },
                        },
                    },
                    "format": {
                        "property": "aggregations.by_player.buckets"
                    },
                },
                "transform": [
                    {"calculate": "datum.key", "as": "player"},
                    {"calculate": "datum.total_xg.value", "as": "xg"},
                    {
                        "calculate": "datum.goals_filter.doc_count",
                        "as": "goals",
                    },
                    {"calculate": "datum.doc_count", "as": "shots"},
                    {
                        "calculate":
                            "datum.top_team.buckets.length > 0"
                            " ? datum.top_team.buckets[0].key : 'Unknown'",
                        "as": "team",
                    },
                    {"filter": "datum.xg >= 0.3"},
                ],
                "mark": {"type": "point", "filled": True, "opacity": 0.8},
                "encoding": {
                    "x": {"field": "xg", "type": "quantitative"},
                    "y": {"field": "goals", "type": "quantitative"},
                    "color": {
                        "field": "team", "type": "nominal",
                        "title": "Team",
                        "scale": {"scheme": "tableau20"},
                    },
                    "size": {
                        "field": "shots", "type": "quantitative",
                        "title": "Total Shots",
                        "scale": {"range": [40, 500]},
                    },
                    "tooltip": [
                        {"field": "player", "title": "Player"},
                        {"field": "team",   "title": "Team"},
                        {"field": "xg",    "title": "xG",    "format": ".2f"},
                        {"field": "goals", "title": "Goals"},
                        {"field": "shots", "title": "Shots"},
                    ],
                },
            },
        ],
        "resolve": {
            "scale": {"color": "independent", "size": "independent"}
        },
        "width": 380, "height": 360, "autosize": "none",
        "title": {
            "text": "Points above the diagonal line are over-performing xG",
            "color": "#888", "fontSize": 10,
        },
    }))

    # ── Goals by match minute ─────────────────────────────────────────────

    vises.append(vega_vis(
        f"{prefix}-goals-by-minute", f"{label} — Goals by Match Minute", {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "data": {
            "url": {
                "index": INDEX,
                "body": {
                    "size": 0,
                    "query": bool_must(
                        cf, {"term": {"shot_outcome": "Goal"}}
                    ),
                    "aggs": {
                        "by_minute": {
                            "histogram": {
                                "field": "minute",
                                "interval": 5,
                                "extended_bounds": {"min": 0, "max": 90},
                            },
                            "aggs": {
                                "by_team": {
                                    "terms": {
                                        "field": "team_name", "size": 8
                                    }
                                }
                            },
                        }
                    },
                },
            },
            "format": {
                "property": "aggregations.by_minute.buckets"
            },
        },
        "transform": [
            {"flatten": ["by_team.buckets"], "as": ["tb"]},
            {"calculate": "datum.key", "as": "minute"},
            {"calculate": "datum.tb.key", "as": "team"},
            {"calculate": "datum.tb.doc_count", "as": "goals"},
        ],
        "mark": {"type": "area", "opacity": 0.7, "line": True},
        "encoding": {
            "x": {
                "field": "minute", "type": "quantitative",
                "title": "Match Minute",
                "scale": {"domain": [0, 90]},
            },
            "y": {
                "field": "goals", "type": "quantitative",
                "title": "Goals", "stack": True,
            },
            "color": {
                "field": "team", "type": "nominal", "title": "Team",
                "scale": {"scheme": "tableau20"},
            },
            "tooltip": [
                {"field": "minute", "title": "Minute (5-min bin)"},
                {"field": "team",   "title": "Team"},
                {"field": "goals",  "title": "Goals"},
            ],
        },
        "width": 420, "height": 260, "autosize": "none",
    }))

    # ── Shot distribution heatmap (pitch overlay) ─────────────────────────

    vises.append(vega_vis(
        f"{prefix}-shot-heatmap", f"{label} — Shot Distribution Heatmap", {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "config": {
            "background": "#2d6b28",
            "view": {"stroke": None},
            "axis": {
                "domain": False, "grid": False,
                "ticks": False, "labels": False, "title": None,
            },
        },
        "layer": [
            {
                "data": {
                    "values": [{"x": 0, "y": 0, "x2": 120, "y2": 80}]
                },
                "mark": {
                    "type": "rect", "color": "#2d6b28", "opacity": 1
                },
                "encoding": {
                    "x":  {"field": "x",  "type": "quantitative",
                           "scale": {"domain": [0, 120]}},
                    "x2": {"field": "x2"},
                    "y":  {"field": "y",  "type": "quantitative",
                           "scale": {"domain": [80, 0]}},
                    "y2": {"field": "y2"},
                },
            },
            {
                "data": {
                    "url": {
                        "index": INDEX,
                        "body": {
                            "size": 0,
                            "query": bool_must(
                                cf, {"term": {"type_name": "Shot"}}
                            ),
                            "aggs": {
                                "by_x": {
                                    "histogram": {
                                        "field": "location_x",
                                        "interval": 8,
                                    },
                                    "aggs": {
                                        "by_y": {
                                            "histogram": {
                                                "field": "location_y",
                                                "interval": 8,
                                            },
                                            "aggs": {
                                                "n": {
                                                    "value_count": {
                                                        "field": "event_id"
                                                    }
                                                },
                                                "avg_xg": {
                                                    "avg": {
                                                        "field": "shot_xg"
                                                    }
                                                },
                                            },
                                        }
                                    },
                                }
                            },
                        },
                    },
                    "format": {
                        "property": "aggregations.by_x.buckets"
                    },
                },
                "transform": [
                    {"flatten": ["by_y.buckets"], "as": ["yb"]},
                    {"calculate": "datum.key",        "as": "x"},
                    {"calculate": "datum.key + 8",    "as": "x2"},
                    {"calculate": "datum.yb.key",     "as": "y"},
                    {"calculate": "datum.yb.key + 8", "as": "y2"},
                    {"calculate": "datum.yb.n.value",      "as": "shots"},
                    {"calculate": "datum.yb.avg_xg.value", "as": "avg_xg"},
                    {"filter": "datum.shots > 0"},
                ],
                "mark": {"type": "rect", "opacity": 0.85},
                "encoding": {
                    "x":  {"field": "x",  "type": "quantitative",
                           "scale": {"domain": [0, 120]}},
                    "x2": {"field": "x2"},
                    "y":  {"field": "y",  "type": "quantitative",
                           "scale": {"domain": [80, 0]}},
                    "y2": {"field": "y2"},
                    "color": {
                        "field": "shots", "type": "quantitative",
                        "scale": {"scheme": "orangered", "type": "sqrt"},
                        "legend": {"title": "Shots"},
                    },
                    "tooltip": [
                        {"field": "x",      "title": "Pitch X Zone"},
                        {"field": "y",      "title": "Pitch Y Zone"},
                        {"field": "shots",  "title": "Shots"},
                        {"field": "avg_xg", "title": "Avg xG",
                         "format": ".3f"},
                    ],
                },
            },
        ],
        "width": 520, "height": 347, "autosize": "none",
    }))

    # ── Interactive shot map (%context% → use dashboard filter bar) ────────

    vises.append(vega_vis(
        f"{prefix}-shot-map", f"{label} — Interactive Shot Map", {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "config": {
            "background": "#1a472a",
            "view": {"stroke": None},
            "axis": {
                "domain": False, "grid": False,
                "ticks": False, "labels": False, "title": None,
            },
        },
        "layer": [
            {
                "data": {
                    "values": [{"x": 0, "y": 0, "x2": 120, "y2": 80}]
                },
                "mark": {
                    "type": "rect", "color": "#2d6b28", "opacity": 1
                },
                "encoding": {
                    "x":  {"field": "x",  "type": "quantitative",
                           "scale": {"domain": [0, 120]}},
                    "x2": {"field": "x2"},
                    "y":  {"field": "y",  "type": "quantitative",
                           "scale": {"domain": [80, 0]}},
                    "y2": {"field": "y2"},
                },
            },
            {
                "data": {
                    "url": {
                        "index": INDEX,
                        "%context%": True,
                        "body": {
                            "size": 5000,
                            "_source": [
                                "location_x", "location_y", "shot_xg",
                                "shot_outcome", "player_name", "team_name",
                                "minute", "type_name", "competition_name",
                            ],
                        },
                    },
                    "format": {"property": "hits.hits"},
                },
                "transform": [
                    {
                        "filter":
                            f"datum._source.competition_name === '{comp_name}'"
                            " && datum._source.type_name === 'Shot'"
                            " && datum._source.location_x != null",
                    },
                    {"calculate": "datum._source.location_x",   "as": "x"},
                    {"calculate": "datum._source.location_y",   "as": "y"},
                    {"calculate": "datum._source.shot_xg",      "as": "xg"},
                    {"calculate": "datum._source.shot_outcome", "as": "outcome"},
                    {"calculate": "datum._source.player_name",  "as": "player"},
                    {"calculate": "datum._source.team_name",    "as": "team"},
                    {"calculate": "datum._source.minute",       "as": "minute"},
                ],
                "mark": {
                    "type": "circle", "opacity": 0.78,
                    "stroke": "white", "strokeWidth": 0.5,
                },
                "encoding": {
                    "x": {"field": "x", "type": "quantitative",
                          "scale": {"domain": [0, 120]}},
                    "y": {"field": "y", "type": "quantitative",
                          "scale": {"domain": [80, 0]}},
                    "color": {
                        "field": "outcome", "type": "nominal",
                        "scale": {
                            "domain": ["Goal", "Saved", "Blocked",
                                       "Off T", "Wayward", "Post"],
                            "range": ["#ff2244", "#ffaa00", "#4499ff",
                                      "#bbbbbb", "#777777", "#ffff44"],
                        },
                        "legend": {"title": "Outcome"},
                    },
                    "size": {
                        "field": "xg", "type": "quantitative",
                        "scale": {"range": [20, 700]},
                        "legend": {"title": "xG"},
                    },
                    "tooltip": [
                        {"field": "player",  "title": "Player"},
                        {"field": "team",    "title": "Team"},
                        {"field": "outcome", "title": "Outcome"},
                        {"field": "xg",     "title": "xG", "format": ".3f"},
                        {"field": "minute", "title": "Minute"},
                    ],
                },
            },
        ],
        "width": 500, "height": 333, "autosize": "none",
        "title": {
            "text": "Filter by team using the dashboard filter bar ↑",
            "color": "#aaa", "fontSize": 10,
        },
    }))

    # ── Player performance table ───────────────────────────────────────────

    vises.append(table_vis(
        f"{prefix}-player-table", f"{label} — Player Table (by xG)",
        aggs=[
            {
                "id": "1", "enabled": True, "type": "terms",
                "schema": "bucket",
                "params": {
                    "field": "player_name.keyword",
                    "orderBy": "2", "order": "desc", "size": 20,
                    "otherBucket": False, "missingBucket": False,
                    "customLabel": "Player",
                },
            },
            {
                "id": "2", "enabled": True, "type": "sum",
                "schema": "metric",
                "params": {"field": "shot_xg", "customLabel": "Total xG"},
            },
            {
                "id": "3", "enabled": True, "type": "count",
                "schema": "metric",
                "params": {"customLabel": "Shots"},
            },
        ],
        kuery=f'{kq} AND type_name : "Shot"',
    ))

    return vises


# ── build visualisations for both competitions ─────────────────────────────

euro_vises = make_competition_visuals(
    "UEFA Euro", "UEFA Euro 2024", "euro", "blues")
wc_vises   = make_competition_visuals(
    "FIFA World Cup", "FIFA World Cup 2022", "wc", "purples")

objects.extend(euro_vises)
objects.extend(wc_vises)

# ── live replay pulse (shared, not competition-specific) ───────────────────

objects.append(metric_vis(
    "viz-replay-pulse", "Live Replay — Events in Window",
    "count", {}, subtitle="Streaming Events",
    index_id=RP_ID))


# ── dashboard factory ──────────────────────────────────────────────────────

def make_dashboard(dash_id, title, prefix, description=""):
    panel_ids = [
        # (panelIndex, vis_suffix,           x,   y,   w,   h)
        ("p01", "kpi-matches",              0,   0,  10,   8),
        ("p02", "kpi-goals",               10,   0,  10,   8),
        ("p03", "kpi-shots",               20,   0,  10,   8),
        ("p04", "kpi-xg",                  30,   0,   9,   8),
        ("p05", "kpi-passes",              39,   0,   9,   8),
        ("p06", "top-scorers",              0,   8,  24,  22),
        ("p07", "xg-scatter",              24,   8,  24,  22),
        ("p08", "top-teams-xg",             0,  30,  24,  20),
        ("p09", "goals-by-minute",         24,  30,  24,  20),
        ("p10", "shot-heatmap",             0,  50,  28,  22),
        ("p11", "player-table",            28,  50,  20,  22),
        ("p12", "shot-map",                 0,  72,  48,  22),
    ]

    panels = []
    refs   = []
    for pi, suffix, x, y, w, h in panel_ids:
        vid = f"{prefix}-{suffix}"
        panels.append({
            "version": "8.12.1",
            "type": "visualization",
            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": pi},
            "panelIndex": pi,
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": f"panel_{pi}",
        })
        refs.append({"name": f"panel_{pi}", "type": "visualization",
                     "id": vid})

    return {
        "type": "dashboard", "id": dash_id,
        "attributes": {
            "title": title,
            "description": description,
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({
                "useMargins": True, "syncColors": False,
                "hidePanelTitles": False,
            }),
            "version": 1, "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                })
            },
        },
        "references": refs,
        "migrationVersion": {"dashboard": "7.9.3"},
        "updated_at": "2024-01-01T00:00:00.000Z",
        "version": "1",
    }


objects.append(make_dashboard(
    "euro-dashboard",
    "UEFA Euro 2024 Analytics",
    "euro",
    description="UEFA Euro 2024 — goals, xG, shots, player and team stats",
))

objects.append(make_dashboard(
    "wc-dashboard",
    "FIFA World Cup 2022 Analytics",
    "wc",
    description="FIFA World Cup 2022 — goals, xG, shots, player and team stats",
))

# Replay dashboard (shared)
objects.append({
    "type": "dashboard", "id": "replay-dashboard",
    "attributes": {
        "title": "Live Match Replay",
        "description": "Real-time streaming from the match replay service",
        "panelsJSON": json.dumps([{
            "version": "8.12.1", "type": "visualization",
            "gridData": {"x": 0, "y": 0, "w": 24, "h": 10, "i": "r01"},
            "panelIndex": "r01",
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": "panel_r01",
        }]),
        "optionsJSON": json.dumps({
            "useMargins": True, "syncColors": False,
            "hidePanelTitles": False,
        }),
        "version": 1, "timeRestore": False,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "query": {"query": "", "language": "kuery"},
                "filter": [],
            })
        },
    },
    "references": [{"name": "panel_r01", "type": "visualization",
                    "id": "viz-replay-pulse"}],
    "migrationVersion": {"dashboard": "7.9.3"},
    "updated_at": "2024-01-01T00:00:00.000Z",
    "version": "1",
})

# ── emit ───────────────────────────────────────────────────────────────────

for obj in objects:
    print(json.dumps(obj))
