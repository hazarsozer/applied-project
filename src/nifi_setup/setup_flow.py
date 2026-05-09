"""
NiFi & Elasticsearch Setup Script
Runs once at startup to:
  1. Create the Elasticsearch football_events index with proper mappings.
  2. Import Kibana saved objects (index patterns + dashboards).
  3. Build and start the StatsBomb NiFi flow via REST API.

NiFi flow pipeline:
  GetFile (/shared/raw/*.json)
    → SplitJson       (split event array → one FlowFile per event)
    → JoltTransformJSON  (flatten deeply nested StatsBomb fields)
    → ReplaceText     (prepend ES bulk-action header line)
    → MergeContent    (batch 100 events into one NDJSON payload)
    → InvokeHTTP      (POST to Elasticsearch /_bulk)

All processors are in nifi-standard-nar — no external NARs required.
"""

import json
import os
import sys
import time
import logging

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nifi-setup")

NIFI_URL  = os.environ.get("NIFI_URL",      "http://nifi:8080")
NIFI_USER = os.environ.get("NIFI_USER",     "nifi_admin")
NIFI_PASS = os.environ.get("NIFI_PASSWORD", "nifi_admin12345")
ES_HOST   = os.environ.get("ES_HOST",       "elasticsearch")
ES_PORT   = os.environ.get("ES_PORT",       "9200")
ES_INDEX  = os.environ.get("ES_INDEX",      "football_events")
ES_URL    = f"http://{ES_HOST}:{ES_PORT}"

JOLT_SPEC_PATH    = os.path.join(os.path.dirname(__file__), "jolt_spec.json")
MAPPINGS_PATH     = "/app/events_mapping.json"  # copied by Dockerfile
KIBANA_URL        = "http://kibana:5601"
SAVED_OBJECTS_PATH = "/app/saved_objects.ndjson"


# ── Elasticsearch setup ───────────────────────────────────────────────────────

def setup_elasticsearch() -> None:
    """Create index template and football_events index if they don't exist."""
    mappings_file = os.path.join(os.path.dirname(__file__), "events_mapping.json")
    if not os.path.exists(mappings_file):
        log.warning("events_mapping.json not found — skipping ES index creation.")
        return

    with open(mappings_file) as f:
        mapping = json.load(f)

    # Create index if absent
    r = requests.head(f"{ES_URL}/{ES_INDEX}", timeout=10)
    if r.status_code == 404:
        r = requests.put(f"{ES_URL}/{ES_INDEX}", json=mapping, timeout=15)
        if r.status_code in (200, 201):
            log.info("Created Elasticsearch index '%s'.", ES_INDEX)
        else:
            log.warning("Could not create ES index: %d %s", r.status_code, r.text[:200])
    else:
        log.info("Elasticsearch index '%s' already exists.", ES_INDEX)


# ── Kibana saved objects ──────────────────────────────────────────────────────

def import_kibana_objects() -> None:
    """Wait for Kibana and import saved objects (index patterns + dashboards)."""
    saved_objects_file = os.path.join(os.path.dirname(__file__), "saved_objects.ndjson")
    if not os.path.exists(saved_objects_file):
        log.warning("saved_objects.ndjson not found — skipping Kibana import.")
        return

    log.info("Waiting for Kibana at %s …", KIBANA_URL)
    for attempt in range(30):
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=10)
            if r.status_code == 200:
                log.info("Kibana ready (attempt %d).", attempt + 1)
                break
        except Exception as exc:
            log.debug("Kibana attempt %d: %s", attempt + 1, exc)
        time.sleep(10)
    else:
        log.warning("Kibana not ready — skipping saved objects import.")
        return

    with open(saved_objects_file, "rb") as f:
        content = f.read()

    r = requests.post(
        f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
        headers={"kbn-xsrf": "true"},
        files={"file": ("saved_objects.ndjson", content, "application/ndjson")},
        timeout=30,
    )
    if r.status_code in (200, 201):
        resp = r.json()
        log.info("Kibana import: %d success, %d errors.",
                 resp.get("successCount", 0), len(resp.get("errors", [])))
    else:
        log.warning("Kibana import returned %d: %s", r.status_code, r.text[:300])


# ── NiFi auth ─────────────────────────────────────────────────────────────────

def _get_token() -> str | None:
    """
    Try to get a bearer token (NiFi HTTPS/secured mode).
    In HTTP/unsecured mode, NiFi allows all API calls without auth —
    return None to signal caller to use no Authorization header.
    """
    try:
        resp = requests.post(
            f"{NIFI_URL}/nifi-api/access/token",
            data={"username": NIFI_USER, "password": NIFI_PASS},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.text.strip()
        # 400/404 = HTTP unsecured mode — no auth needed
        log.info("NiFi auth endpoint returned %d — running in unsecured HTTP mode.",
                 resp.status_code)
        return None
    except Exception as exc:
        log.warning("NiFi token request failed (%s) — assuming unsecured mode.", exc)
        return None


def _h(token: str | None) -> dict:
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


# ── NiFi REST helpers ─────────────────────────────────────────────────────────

def get_root_id(token: str) -> str:
    r = requests.get(f"{NIFI_URL}/nifi-api/process-groups/root", headers=_h(token), timeout=10)
    r.raise_for_status()
    return r.json()["id"]


def create_pg(token: str, parent_id: str, name: str, x: float = 0, y: float = 0) -> dict:
    body = {"revision": {"version": 0},
            "component": {"name": name, "position": {"x": x, "y": y}}}
    r = requests.post(f"{NIFI_URL}/nifi-api/process-groups/{parent_id}/process-groups",
                      headers=_h(token), json=body, timeout=15)
    r.raise_for_status()
    return r.json()


def create_proc(token: str, pg_id: str, ptype: str, name: str, props: dict,
                x: float, y: float, period: str = "0 sec",
                auto_term: list | None = None) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "type": ptype,
            "name": name,
            "position": {"x": x, "y": y},
            "config": {
                "properties": props,
                "schedulingPeriod": period,
                "schedulingStrategy": "TIMER_DRIVEN",
                "autoTerminatedRelationships": auto_term or [],
            },
        },
    }
    r = requests.post(f"{NIFI_URL}/nifi-api/process-groups/{pg_id}/processors",
                      headers=_h(token), json=body, timeout=15)
    if r.status_code not in (200, 201):
        log.error("create_proc %s → %d %s", name, r.status_code, r.text[:400])
        r.raise_for_status()
    return r.json()


def connect(token: str, pg_id: str,
            src: str, src_t: str,
            dst: str, dst_t: str,
            rels: list) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "source":      {"id": src, "type": src_t, "groupId": pg_id},
            "destination": {"id": dst, "type": dst_t, "groupId": pg_id},
            "selectedRelationships": rels,
            "backPressureDataSizeThreshold": "1 GB",
            "backPressureObjectThreshold": "10000",
        },
    }
    r = requests.post(f"{NIFI_URL}/nifi-api/process-groups/{pg_id}/connections",
                      headers=_h(token), json=body, timeout=15)
    if r.status_code not in (200, 201):
        log.error("connect %s→%s → %d %s", src, dst, r.status_code, r.text[:300])
        r.raise_for_status()
    return r.json()


def start_pg(token: str, pg_id: str) -> None:
    r = requests.put(f"{NIFI_URL}/nifi-api/flow/process-groups/{pg_id}",
                     headers=_h(token),
                     json={"id": pg_id, "state": "RUNNING"}, timeout=15)
    r.raise_for_status()


def flow_exists(token: str, root_id: str) -> bool:
    r = requests.get(f"{NIFI_URL}/nifi-api/process-groups/{root_id}/process-groups",
                     headers=_h(token), timeout=10)
    r.raise_for_status()
    return any(g["component"]["name"] == "StatsBomb Football Pipeline"
               for g in r.json().get("processGroups", []))


# ── NiFi flow builder ─────────────────────────────────────────────────────────

P = "PROCESSOR"

def build_nifi_flow(token: str, root_id: str) -> None:
    with open(JOLT_SPEC_PATH) as f:
        jolt_spec = json.dumps(json.load(f))

    bulk_header = '{"index":{"_index":"' + ES_INDEX + '"}}\n$1\n'

    pg = create_pg(token, root_id, "StatsBomb Football Pipeline", x=150, y=150)
    pg_id = pg["id"]
    log.info("Created NiFi process group %s", pg_id)

    # 1 ── GetFile: poll /shared/raw for event JSON files from Airflow
    gf = create_proc(
        token, pg_id,
        "org.apache.nifi.processors.standard.GetFile",
        "GetFile (StatsBomb raw events)",
        props={
            "Input Directory":  "/shared/raw",
            "File Filter":      "[\\s\\S]*\\.json",
            "Keep Source File": "false",
            "Minimum File Age": "2 sec",
            "Batch Size":       "1",
        },
        x=50, y=50, period="15 sec",
    )

    # 2 ── SplitJson: explode array of events → one FlowFile per event
    sj = create_proc(
        token, pg_id,
        "org.apache.nifi.processors.standard.SplitJson",
        "SplitJson (events array)",
        props={"JsonPath Expression": "$.*"},
        x=50, y=200,
        auto_term=["original", "failure"],
    )

    # 3 ── JoltTransformJSON: flatten deeply nested StatsBomb structure
    jolt = create_proc(
        token, pg_id,
        "org.apache.nifi.processors.standard.JoltTransformJSON",
        "JoltTransformJSON (flatten nested fields)",
        props={
            "jolt-transform": "jolt-transform-chain",
            "jolt-spec":      jolt_spec,
        },
        x=50, y=350,
        auto_term=["failure"],
    )

    # 4 ── ReplaceText: prepend ES bulk action header before each event doc
    rt = create_proc(
        token, pg_id,
        "org.apache.nifi.processors.standard.ReplaceText",
        "ReplaceText (ES bulk header)",
        props={
            "Replacement Strategy": "Regex Replace",
            "Regular Expression":   "([\\s\\S]+)",
            "Replacement Value":    bulk_header,
            "Evaluation Mode":      "Entire text",
        },
        x=50, y=500,
        auto_term=["failure"],
    )

    # 5 ── MergeContent: batch up to 100 events → single NDJSON payload
    mc = create_proc(
        token, pg_id,
        "org.apache.nifi.processors.standard.MergeContent",
        "MergeContent (batch 100 events)",
        props={
            "Merge Strategy":             "Bin-Packing Algorithm",
            "Merge Format":               "Binary Concatenation",
            "Minimum Number of Entries":  "1",
            "Maximum Number of Entries":  "100",
            "Max Bin Age":                "10 sec",
        },
        x=50, y=650,
        auto_term=["original", "failure"],
    )

    # 6 ── InvokeHTTP: POST NDJSON batch to Elasticsearch _bulk endpoint
    ih = create_proc(
        token, pg_id,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "InvokeHTTP (→ Elasticsearch _bulk)",
        props={
            "Remote URL":         f"http://{ES_HOST}:{ES_PORT}/_bulk",
            "HTTP Method":        "POST",
            "Content-Type":       "application/x-ndjson",
            "Connection Timeout": "5 secs",
            "Read Timeout":       "30 secs",
            "Include Date Header": "True",
        },
        x=50, y=800,
        auto_term=["Response", "Failure", "No Retry", "Retry", "Original"],
    )

    # ── Wire processors ───────────────────────────────────────────────────────
    connect(token, pg_id, gf["id"],   P, sj["id"],   P, ["success"])
    connect(token, pg_id, sj["id"],   P, jolt["id"], P, ["split"])
    connect(token, pg_id, jolt["id"], P, rt["id"],   P, ["success"])
    connect(token, pg_id, rt["id"],   P, mc["id"],   P, ["success"])
    connect(token, pg_id, mc["id"],   P, ih["id"],   P, ["merged"])

    log.info("NiFi flow wired. Starting process group…")
    start_pg(token, pg_id)
    log.info("StatsBomb Football Pipeline is RUNNING in NiFi.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    # 1. Elasticsearch index + mappings
    log.info("Setting up Elasticsearch index '%s'…", ES_INDEX)
    setup_elasticsearch()

    # 2. Kibana dashboards (runs in background — Kibana may still be starting)
    try:
        import_kibana_objects()
    except Exception as exc:
        log.warning("Kibana import failed (non-fatal): %s", exc)

    # 3. NiFi flow
    log.info("Waiting for NiFi at %s …", NIFI_URL)
    token = None
    nifi_ready = False
    for attempt in range(50):
        try:
            # Probe the system-diagnostics endpoint (always available)
            r = requests.get(f"{NIFI_URL}/nifi-api/system-diagnostics", timeout=10)
            if r.status_code in (200, 401, 403):
                # 200 = open, 401/403 = secured (need auth)
                nifi_ready = True
                token = _get_token()
                log.info("NiFi ready (attempt %d, secured=%s).", attempt + 1, token is not None)
                break
        except Exception as exc:
            log.debug("Attempt %d: %s", attempt + 1, exc)
        time.sleep(10)

    if not nifi_ready:
        log.error("NiFi did not become ready after 50 attempts. Exiting.")
        sys.exit(1)

    root_id = get_root_id(token)
    log.info("NiFi root PG: %s", root_id)

    if flow_exists(token, root_id):
        log.info("NiFi flow already exists — skipping build.")
    else:
        build_nifi_flow(token, root_id)

    for d in ["/shared/raw", "/shared/processed", "/shared/nifi_trigger"]:
        os.makedirs(d, exist_ok=True)
        os.chmod(d, 0o777)
    os.chmod("/shared", 0o777)
    log.info("All setup tasks complete.")


if __name__ == "__main__":
    main()
