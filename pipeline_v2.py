"""
pipeline_v2.py — Enhanced Incremental Pipeline
================================================
Adds: State, Region, Language, Use-case (Campaign Flag), Campaign Name
Hour: from call_info.StartTime in Indian AM/PM format (IST)
DBs:  AUTO-DISCOVERS all databases from the cluster (skips system DBs)
      Override with INCLUDE_DBS or EXCLUDE_DBS lists below.
Cron: safe to run every hour — only fetches NEW sessions via checkpoint

FALLBACK LOGIC:
  Step 1 -> use user_info.row.STATE / REGION / CAMPAIGN_FLAG / CAMPAIGN_NAME
  Step 2 -> if missing, infer STATE+REGION from model_data.language_detected
"""

import csv
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

from pymongo import MongoClient

# ================================================================
# STEP 1 — Configuration
# ================================================================
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://wordworksai:GRXtnwKRM4qazkkF@"
    "wwai-prod-main-stable-db-1.global.mongocluster.cosmos.azure.com/"
)

# ── DB discovery ────────────────────────────────────────────────
# Leave INCLUDE_DBS empty [] to auto-discover ALL databases.
# Add specific names to INCLUDE_DBS to restrict to only those DBs.
# Add names to EXCLUDE_DBS to skip certain DBs (system DBs auto-excluded).
INCLUDE_DBS = [
    "agrim",
    "bajaj",
    "cred",
    "dbs_mintek",
    "dubai_expats",
    "ecofy",
    "hdfc",
    "housing",
    "inprime",
    "it_cart",
    "kissht",
    "micro_fi",
    "mktg_micro_fi",
    "multify",
    "opus",
    "salesbot_datasheets",
    "shubham_housing",
    "smart_dial",
    "tatacapital",
    "ugro",
]
EXCLUDE_DBS = []    # not needed when INCLUDE_DBS is set

# These are always skipped regardless of INCLUDE_DBS
SYSTEM_DBS = {"admin", "local", "config"}

COLLECTION_NAME   = "sessions"    # same across all 20 DBs
EXECUTIONS_COLL   = "executions"  # same across all 20 DBs — used to fetch valid execution_ids

# NOTE: total sessions fetched from sessions collection == Total_calls in the CSV output

SUCCESS_DISPOSITIONS = {
    "PTP", "PAID", "FPTP", "CLAIM_PAID",
    "ALREADY_PAID", "CLBK", "CB", "LM",
}
FAILURE_DISPOSITIONS = {
    "RTP", "RTPF", "NC", "NR", "GREETING ONLY",
    "HANG_UP", "DISPUTE", "JOBLESS", "DEATH", "DIF", "ME", "LB",
}

CONNECTED_THRESHOLD_SEC = 5
CHECKPOINT_FILE = "checkpoint.json"
OUTPUT_FILE     = "session_analytics_realtime.csv"

# How many hours to look back for executions on every incremental run.
# Catches executions that were missed due to transient errors or that were
# created just before/after a checkpoint boundary.
LOOKBACK_HOURS = 48

CSV_HEADER = [
    "Company name", "State", "Use case wise", "Language", "Region",
    "Date", "Hour",
    "Total_calls", "Connected_calls", "Connected rate",
    "Total_duration_seconds", "Total_duration_minutes", "Total_duration_hours",
    "Positive disposition", "Negative disposition", "Other disposition",
    "Success rate", "Listen Count", "Listen rate",
]

# ================================================================
# STEP 2 — Fallback: Language -> Region / State
# ================================================================
LANG_TO_REGION = {
    "hindi":     "NORTH",
    "punjabi":   "NORTH",
    "haryanvi":  "NORTH",
    "urdu":      "NORTH",
    "tamil":     "SOUTH",
    "telugu":    "SOUTH",
    "kannada":   "SOUTH",
    "malayalam": "SOUTH",
    "marathi":   "WEST",
    "gujarati":  "WEST",
    "bengali":   "EAST",
    "odia":      "EAST",
    "assamese":  "EAST",
}

LANG_TO_STATE = {
    "tamil":     "TAMIL NADU",
    "telugu":    "ANDHRA PRADESH",
    "kannada":   "KARNATAKA",
    "malayalam": "KERALA",
    "marathi":   "MAHARASHTRA",
    "gujarati":  "GUJARAT",
    "bengali":   "WEST BENGAL",
    "punjabi":   "PUNJAB",
    "odia":      "ODISHA",
    "assamese":  "ASSAM",
}

# ================================================================
# STEP 2b — Data normalization maps
# ================================================================

# Language: normalise spelling variants -> canonical key used in LANG_TO_REGION/STATE
LANG_NORMALIZE = {
    # Telugu variants
    "telegu": "telugu", "telulu": "telugu", "te": "telugu",
    # Tamil variants
    "tamizh": "tamil", "tamilmad": "tamil",
    # Odia variants
    "odiya": "odia", "oriya": "odia", "odisha": "odia", "or": "odia",
    # Kannada variants
    "kannda": "kannada", "kn": "kannada",
    # Hindi variants
    "hi": "hindi", "hinglish": "hindi",
    # Mixed / junk -> unknown
    "bilingual": "unknown", "both": "unknown", "mixed": "unknown",
    "multi": "unknown", "multi_language": "unknown", "multilingual": "unknown",
    "multiple": "unknown", "inconclusive": "unknown", "other": "unknown",
    "none": "unknown", "string": "unknown",
}

def normalize_language(raw: str) -> str:
    """Normalise raw language_detected to a clean canonical value."""
    s = raw.strip().lower()
    # Pure junk: digits, symbols, empty
    if not s or s in ("unknown", "") or s.replace("?", "").strip() == "":
        return "unknown"
    try:
        float(s)          # catches "1", "16", "19.01", "0.2333" etc.
        return "unknown"
    except ValueError:
        pass
    # Direct map
    if s in LANG_NORMALIZE:
        return LANG_NORMALIZE[s]
    # If value contains "/" or "-" or "," it's a mix -> keep first recognised token
    for sep in ("/", "-", ",", "_", " "):
        if sep in s:
            first = s.split(sep)[0].strip()
            first = LANG_NORMALIZE.get(first, first)
            if first in LANG_TO_REGION or first in LANG_TO_STATE:
                return first
            return "unknown"
    return s   # already clean single-language value


# Region: merge zone aliases and strip junk
REGION_NORMALIZE = {
    "east zone":        "EAST",
    "north zone":       "NORTH",
    "west zone":        "WEST",
    "south zone":       "SOUTH",
    "north-1":          "NORTH",
    "north-2":          "NORTH",
    "south -1(tn+kr)":  "SOUTH",
    "south -2(ap+tln)": "SOUTH",
    "india":            "UNKNOWN",
    "dubai":            "UNKNOWN",   # city, not an Indian zone
    "uae":              "UNKNOWN",
}
REGION_JUNK = {"**", "***", "50.0", ""}

def normalize_region(raw: str) -> str:
    s = raw.strip()
    if s.upper() == "UNKNOWN" or s in REGION_JUNK:
        return "UNKNOWN"
    lo = s.lower()
    if lo in REGION_NORMALIZE:
        return REGION_NORMALIZE[lo]
    # Only accept known canonical values
    if s.upper() in {"NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"}:
        return s.upper()
    return "UNKNOWN"


# Use-case: merge near-duplicates and strip junk
USECASE_NORMALIZE = {
    "gen-ai-bot":    "GEN AI BOT",
    "bkt x bot":     "BKT X",
    "bucket_x":      "BUCKET_X",
}
USECASE_JUNK = {"**", "***", "2", "abc", ""}

def normalize_usecase(raw: str) -> str:
    s = raw.strip()
    if s.upper() == "UNKNOWN" or s in USECASE_JUNK:
        return "UNKNOWN"
    lo = s.lower()
    if lo in USECASE_NORMALIZE:
        return USECASE_NORMALIZE[lo]
    return s.upper()


# State: normalise common typos / city names used as states
STATE_NORMALIZE = {
    "andra":           "ANDHRA PRADESH",
    "andra pradesh":   "ANDHRA PRADESH",
    "ap":              "ANDHRA PRADESH",
    "tamilnadu":       "TAMIL NADU",
    "maharastra":      "MAHARASHTRA",
    "gujrat":          "GUJARAT",
    "harayana":        "HARYANA",
    "madhyapradesh":   "MADHYA PRADESH",
    "mp":              "MADHYA PRADESH",
    "up":              "UTTAR PRADESH",
    "uttranchal":      "UTTARAKHAND",
    "orrisa":          "ODISHA",
    "orissa":          "ODISHA",
    "new delhi":       "DELHI",
    "west delhi":      "DELHI",
    # cities used as state names -> map to state
    "mumbai":          "MAHARASHTRA",
    "pune":            "MAHARASHTRA",
    "chennai":         "TAMIL NADU",
    "bangalore":       "KARNATAKA",
    "hubli":           "KARNATAKA",
    "indore":          "MADHYA PRADESH",
    "raipur":          "CHHATTISGARH",
    "jaipur":          "RAJASTHAN",
    "kalyan":          "MAHARASHTRA",
    "hisar":           "HARYANA",
    "chattisgarh":     "CHHATTISGARH",
    "pch":             "UNKNOWN",
    "kt":              "UNKNOWN",
}
STATE_JUNK = {"**", "***", ""}

def normalize_state(raw: str) -> str:
    s = raw.strip()
    if s.upper() == "UNKNOWN" or s in STATE_JUNK:
        return "UNKNOWN"
    lo = s.lower()
    if lo in STATE_NORMALIZE:
        return STATE_NORMALIZE[lo]
    return s.upper()


# ================================================================
# STEP 3 — Time helpers
# ================================================================
def parse_start_time(start_time_str: str):
    """Parse call_info.StartTime string -> datetime object."""
    if not start_time_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(start_time_str, fmt)
        except ValueError:
            pass
    return None


def to_indian_ampm_hour(dt: datetime) -> str:
    """
    Convert datetime hour to Indian AM/PM label.
    e.g. 15:49 -> '03:00:00 PM'
         09:05 -> '09:00:00 AM'
    StartTime is already in IST (UTC+5:30), no conversion needed.
    """
    h = dt.hour
    if h == 0:
        return "12:00:00 AM"
    elif h < 12:
        return f"{h:02d}:00:00 AM"
    elif h == 12:
        return "12:00:00 PM"
    else:
        return f"{h - 12:02d}:00:00 PM"


def int_to_indian_ampm(h: int) -> str:
    """Convert hour integer (0-23) to Indian AM/PM label."""
    if h == 0:
        return "12:00:00 AM"
    elif h < 12:
        return f"{h:02d}:00:00 AM"
    elif h == 12:
        return "12:00:00 PM"
    else:
        return f"{h - 12:02d}:00:00 PM"


def format_date(dt: datetime) -> str:
    """DD-MM-YYYY to match Mainrowdata.csv format."""
    return dt.strftime("%d-%m-%Y")


# ================================================================
# STEP 4 — Checkpoint (tracks last_time + already-processed exec IDs)
# ================================================================
# New checkpoint format per DB:
#   { "last_time": "<ISO>", "done_ids": ["id1", "id2", ...] }
#
# Old format was just a plain ISO string.  load_checkpoint() migrates
# automatically: old string → new dict with done_ids = [] (empty).
# The first run after migration will use the 48h lookback and may find
# some executions that were already processed; those will be re-added to
# done_ids so subsequent runs skip them cleanly.
# ================================================================

def load_checkpoint() -> dict:
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    with open(CHECKPOINT_FILE, "r") as f:
        raw = json.load(f)
    # Migrate old format: string → new dict
    result = {}
    for db, val in raw.items():
        if isinstance(val, str):
            result[db] = {"last_time": val, "done_ids": []}
        else:
            result[db] = val
    return result


MAX_DONE_IDS = 5_000  # per DB — ~200 days @ 25 exec/day; keeps file small

def save_checkpoint(data: dict):
    # Trim done_ids lists to prevent unbounded growth
    for entry in data.values():
        if isinstance(entry, dict) and "done_ids" in entry:
            ids = entry["done_ids"]
            if len(ids) > MAX_DONE_IDS:
                entry["done_ids"] = ids[-MAX_DONE_IDS:]   # keep newest
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, default=str, indent=2)


# ================================================================
# STEP 5 — MongoDB projection (fetch only needed fields)
# ================================================================
PROJECTION = {
    "_id":                              0,
    "execution_id":                     1,
    "created_at":                       1,
    # Time from actual call
    "call_info.StartTime":              1,
    "call_info.Duration":               1,
    # Disposition & language
    "model_data.disposition_code":      1,
    "model_data.language_detected":     1,
    # Row fields (State, Region, Campaign)
    "user_info.row.STATE":              1,
    "user_info.row.REGION":             1,
    "user_info.row.CAMPAIGN_NAME":      1,
    "user_info.row.CAMPAIGN_FLAG":      1,
}


# ================================================================
# STEP 6 — Process a single DB incrementally
# ================================================================
def get_execution_ids(db, done_ids: set, is_first_run: bool) -> list:
    """
    First run  → fetch ALL execution_ids (full history).
    Subsequent → fetch executions from the last LOOKBACK_HOURS window
                 (catches missed/late executions), then remove any IDs
                 already in done_ids so they are not double-counted.

    Why lookback instead of strict > last_time?
      An execution may have been created just before a checkpoint boundary
      or during a transient DB error — strict filtering would miss it forever.
      The done_ids set ensures previously processed IDs are skipped.
    """
    try:
        exec_col = db[EXECUTIONS_COLL]
        if is_first_run:
            ids = exec_col.distinct("_id")      # ALL history on first run
        else:
            lookback_time = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
            ids = exec_col.distinct("_id", {"created_at": {"$gt": lookback_time}})
            # Filter out IDs we've already processed (prevents double-counting)
            ids = [i for i in ids if str(i) not in done_ids]
        return [str(i) for i in ids]
    except Exception as e:
        print(f"    [warn] Could not fetch execution_ids: {e} — falling back to sessions query")
        return []


# Max execution_ids per aggregation batch — keeps each query fast on CosmosDB
# micro_fi/tatacapital have ~5700 sessions per exec_id, so 10×5700 ≈ 57k per chunk
EXEC_CHUNK_SIZE = 10

# Aggregation pipeline template (match stage is injected per chunk)
_AGG_STAGES = [
    {"$addFields": {
        "_start_dt": {"$cond": {
            "if":   {"$ne": [{"$ifNull": ["$call_info.StartTime", ""]}, ""]},
            "then": {"$dateFromString": {
                "dateString": "$call_info.StartTime",
                "format":     "%Y-%m-%d %H:%M:%S",
                "onError":    {"$ifNull": ["$created_at", datetime(2020, 1, 1)]},
                "onNull":     {"$ifNull": ["$created_at", datetime(2020, 1, 1)]},
            }},
            "else": {"$ifNull": ["$created_at", datetime(2020, 1, 1)]},
        }},
        "_dur":    {"$convert": {"input": "$call_info.Duration", "to": "int", "onError": 0, "onNull": 0}},
        "_disp":   {"$toUpper": {"$ifNull": ["$model_data.disposition_code", ""]}},
        "_lang":   {"$toLower": {"$ifNull": ["$model_data.language_detected", "unknown"]}},
        "_state":  {"$ifNull":  ["$user_info.row.STATE",         ""]},
        "_region": {"$ifNull":  ["$user_info.row.REGION",        ""]},
        "_flag":   {"$ifNull":  ["$user_info.row.CAMPAIGN_FLAG", "UNKNOWN"]},
        "_camp":   {"$ifNull":  ["$user_info.row.CAMPAIGN_NAME", "UNKNOWN"]},
    }},
    {"$addFields": {
        "_hour_int": {"$hour":         "$_start_dt"},
        "_date_str": {"$dateToString": {"format": "%d-%m-%Y", "date": "$_start_dt"}},
    }},
    {"$group": {
        "_id": {
            "date":     "$_date_str",
            "hour":     "$_hour_int",
            "state":    "$_state",
            "region":   "$_region",
            "language": "$_lang",
            "use_case": "$_flag",
            "campaign": "$_camp",
        },
        "total":     {"$sum": 1},
        "dur_sec":   {"$sum": "$_dur"},
        "connected": {"$sum": {"$cond": [{"$gt": ["$_dur", CONNECTED_THRESHOLD_SEC]}, 1, 0]}},
        "success":   {"$sum": {"$cond": [{"$in": ["$_disp", list(SUCCESS_DISPOSITIONS)]}, 1, 0]}},
        "failure":   {"$sum": {"$cond": [{"$in": ["$_disp", list(FAILURE_DISPOSITIONS)]}, 1, 0]}},
    }},
]


def _run_agg_chunk(collection, match_filter: dict) -> list:
    """Run one aggregation chunk. Raises on error."""
    pipeline = [{"$match": match_filter}] + _AGG_STAGES
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def _merge_agg_result(rows: dict, results: list, db_name: str):
    """Merge aggregation results into the shared rows dict (sums matching keys)."""
    for doc in results:
        g   = doc["_id"]
        tot = doc["total"]

        lang     = normalize_language(g.get("language") or "unknown")
        raw_state  = str(g.get("state")  or "").strip()
        raw_region = str(g.get("region") or "").strip()
        state  = normalize_state(raw_state)  if raw_state  else LANG_TO_STATE.get(lang,  "UNKNOWN")
        region = normalize_region(raw_region) if raw_region else LANG_TO_REGION.get(lang, "UNKNOWN")
        # If normalization returned UNKNOWN, try language fallback
        if state  == "UNKNOWN": state  = LANG_TO_STATE.get(lang,  "UNKNOWN")
        if region == "UNKNOWN": region = LANG_TO_REGION.get(lang, "UNKNOWN")

        key = (
            db_name,
            g.get("date") or "01-01-2020",
            int_to_indian_ampm(g.get("hour") or 0),
            state, region, lang,
            normalize_usecase(g.get("use_case") or ""),
            g.get("campaign") or "UNKNOWN",
        )
        if key in rows:
            r = rows[key]
            r["total"]        += tot
            r["connected"]    += doc["connected"]
            r["duration_sec"] += doc["dur_sec"]
            r["success"]      += doc["success"]
            r["failure"]      += doc["failure"]
            r["other"]        += max(0, tot - doc["success"] - doc["failure"])
            r["listen"]       += doc["success"]
        else:
            rows[key] = {
                "total":        tot,
                "connected":    doc["connected"],
                "duration_sec": doc["dur_sec"],
                "success":      doc["success"],
                "failure":      doc["failure"],
                "other":        max(0, tot - doc["success"] - doc["failure"]),
                "listen":       doc["success"],
            }


def process_db(db_name: str, db, done_ids: set, last_time: datetime, is_first_run: bool) -> tuple:
    """
    Server-side aggregation in chunks of EXEC_CHUNK_SIZE execution_ids.
    Each chunk runs a separate $group so no single query times out on CosmosDB.
    Falls back to cursor scan only if aggregation fails on a chunk.

    Returns (rows_dict, processed_exec_ids) so the caller can update done_ids.
    """
    collection = db[COLLECTION_NAME]
    exec_ids = get_execution_ids(db, done_ids, is_first_run)

    if exec_ids:
        n_chunks = (len(exec_ids) + EXEC_CHUNK_SIZE - 1) // EXEC_CHUNK_SIZE
        print(f"  [{db_name}] {len(exec_ids)} execution_ids -> {n_chunks} chunk(s) | server-side aggregation ...")
        chunks = [exec_ids[i:i + EXEC_CHUNK_SIZE] for i in range(0, len(exec_ids), EXEC_CHUNK_SIZE)]
    elif is_first_run:
        print(f"  [{db_name}] No executions — full collection scan server-side ...")
        chunks = [None]   # single pass, no exec_id filter
    else:
        # No new execution_ids in the lookback window — but late-arriving
        # sessions (uploaded after the last run) may still exist in old
        # executions.  Fall back to a direct session-level created_at query.
        print(f"  [{db_name}] No new executions — querying sessions by created_at > {last_time.strftime('%Y-%m-%d %H:%M')} ...")
        direct_filter = {"created_at": {"$gt": last_time}}
        rows: dict = {}
        try:
            results = _run_agg_chunk(collection, direct_filter)
            _merge_agg_result(rows, results, db_name)
            n_sess = sum(v["total"] for v in rows.values())
            print(f"  [{db_name}] Direct session query -> {n_sess:,} new sessions -> {len(rows):,} rows")
        except Exception as e:
            print(f"  [{db_name}] Direct agg failed ({type(e).__name__}) — cursor fallback ...")
            rows = _process_db_cursor(db_name, collection, direct_filter)
        return rows, []   # no new exec_ids to mark as done

    rows: dict = {}
    total_sessions = 0

    for idx, chunk in enumerate(chunks, 1):
        # Build match filter for this chunk
        if chunk is not None:
            # Filter sessions by execution_id only — execution_ids were already
            # filtered by created_at in get_execution_ids(), so sessions for new
            # executions must not be further filtered by session.created_at (calls
            # happened earlier but were uploaded/processed later).
            match_filter = {"execution_id": {"$in": chunk}}
        else:
            match_filter = {} if is_first_run else None

        if match_filter is None:
            continue

        try:
            results = _run_agg_chunk(collection, match_filter)
            chunk_total = sum(d["total"] for d in results)
            total_sessions += chunk_total
            _merge_agg_result(rows, results, db_name)
            if len(chunks) > 1:
                print(f"  [{db_name}] chunk {idx}/{len(chunks)} -> {chunk_total:,} sessions")
        except Exception as e:
            print(f"  [{db_name}] chunk {idx} agg failed ({type(e).__name__}) — cursor fallback for this chunk ...")
            chunk_rows = _process_db_cursor(db_name, collection, match_filter)
            # merge cursor rows into main rows dict
            for key, agg in chunk_rows.items():
                if key in rows:
                    for k in agg:
                        rows[key][k] += agg[k]
                else:
                    rows[key] = agg
            total_sessions += sum(v["total"] for v in chunk_rows.values())

    print(f"  [{db_name}] Done — {total_sessions:,} sessions -> {len(rows):,} aggregated rows")
    # Return the rows AND the list of exec_ids that were just processed,
    # so run_pipeline() can add them to done_ids in the checkpoint.
    return rows, exec_ids if exec_ids else []


def _process_db_cursor(db_name: str, collection, match_filter: dict) -> dict:
    """Fallback: fetch raw docs and aggregate in Python (used if $group fails)."""
    cursor = collection.find(match_filter, PROJECTION).batch_size(2000)
    rows = defaultdict(lambda: {
        "total": 0, "connected": 0, "duration_sec": 0,
        "success": 0, "failure": 0, "other": 0, "listen": 0,
    })
    count = 0
    for doc in cursor:
        count += 1
        if count % 50_000 == 0:
            print(f"  [{db_name}] ... {count:,} sessions so far (cursor fallback) ...")

        created_at = doc.get("created_at")
        if isinstance(created_at, dict):
            try:
                created_at = datetime.fromisoformat(created_at["$date"].replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue
        elif isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace("Z", ""))
            except Exception:
                continue
        if not isinstance(created_at, datetime):
            continue

        call_info  = doc.get("call_info") or {}
        model_data = doc.get("model_data") or {}
        user_info  = doc.get("user_info") or {}
        row_data   = user_info.get("row") or {}

        try:
            duration = int(call_info.get("Duration") or 0)
        except (ValueError, TypeError):
            duration = 0

        start_dt = parse_start_time(call_info.get("StartTime", ""))
        if start_dt:
            hour_label = to_indian_ampm_hour(start_dt)
            date_label = format_date(start_dt)
        else:
            hour_label = to_indian_ampm_hour(created_at)
            date_label = format_date(created_at)

        disposition = str(model_data.get("disposition_code") or "").strip().upper()
        language    = normalize_language(str(model_data.get("language_detected") or "unknown"))
        raw_state   = str(row_data.get("STATE")  or "").strip()
        raw_region  = str(row_data.get("REGION") or "").strip()
        state  = normalize_state(raw_state)   if raw_state  else LANG_TO_STATE.get(language,  "UNKNOWN")
        region = normalize_region(raw_region) if raw_region else LANG_TO_REGION.get(language, "UNKNOWN")
        if state  == "UNKNOWN": state  = LANG_TO_STATE.get(language,  "UNKNOWN")
        if region == "UNKNOWN": region = LANG_TO_REGION.get(language, "UNKNOWN")
        use_case = normalize_usecase(str(row_data.get("CAMPAIGN_FLAG") or ""))
        campaign = str(row_data.get("CAMPAIGN_NAME") or "UNKNOWN").strip()

        if disposition in SUCCESS_DISPOSITIONS:
            disp_type = "success"
        elif disposition in FAILURE_DISPOSITIONS:
            disp_type = "failure"
        else:
            disp_type = "other"

        key = (db_name, date_label, hour_label, state, region, language, use_case, campaign)
        agg = rows[key]
        agg["total"]        += 1
        agg["duration_sec"] += duration
        if duration > CONNECTED_THRESHOLD_SEC:
            agg["connected"] += 1
        if disp_type == "success":
            agg["success"] += 1
            agg["listen"]  += 1
        elif disp_type == "failure":
            agg["failure"] += 1
        else:
            agg["other"]   += 1

    print(f"  [{db_name}] Processed {count:,} sessions -> {len(rows):,} rows (cursor fallback)")
    return dict(rows)


# ================================================================
# STEP 7 — Write aggregated rows to CSV  (merge / upsert mode)
# ================================================================
def write_to_csv(all_rows: dict, output_file: str):
    """
    Merge new rows into the CSV instead of blindly appending.

    Every run:
      1. Read the existing CSV into a keyed dict.
      2. For each new row: if the key already exists → sum the counts;
         otherwise → insert as a new row.
      3. Re-write the whole CSV from the merged dict.

    This means every day's totals are always up-to-date: a new execution
    whose sessions belong to yesterday automatically increments yesterday's
    row rather than creating a duplicate.

    Merge key: (Company name, State, Use case wise, Language, Region, Date, Hour)
    Campaign name is intentionally excluded — it is not a CSV column.
    """
    # ── 1. Load existing CSV ──────────────────────────────────────
    existing: dict = {}          # csv_key -> agg dict
    if os.path.exists(output_file):
        with open(output_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                k = (
                    row.get("Company name", ""),
                    row.get("State", ""),
                    row.get("Use case wise", ""),
                    row.get("Language", ""),
                    row.get("Region", ""),
                    row.get("Date", ""),
                    row.get("Hour", ""),
                )
                existing[k] = {
                    "total":        int(float(row.get("Total_calls",           0) or 0)),
                    "connected":    int(float(row.get("Connected_calls",       0) or 0)),
                    "duration_sec": float(row.get("Total_duration_seconds",    0) or 0),
                    "success":      int(float(row.get("Positive disposition",  0) or 0)),
                    "failure":      int(float(row.get("Negative disposition",  0) or 0)),
                    "other":        int(float(row.get("Other disposition",     0) or 0)),
                    "listen":       int(float(row.get("Listen Count",          0) or 0)),
                }

    # ── 2. Merge new rows (upsert) ────────────────────────────────
    for key, agg in all_rows.items():
        db_name, date_label, hour_label, state, region, language, use_case, _campaign = key
        csv_key = (db_name, state, use_case, language, region, date_label, hour_label)
        if csv_key in existing:
            ex = existing[csv_key]
            ex["total"]        += agg["total"]
            ex["connected"]    += agg["connected"]
            ex["duration_sec"] += agg["duration_sec"]
            ex["success"]      += agg["success"]
            ex["failure"]      += agg["failure"]
            ex["other"]        += agg["other"]
            ex["listen"]       += agg["listen"]
        else:
            existing[csv_key] = {
                "total":        agg["total"],
                "connected":    agg["connected"],
                "duration_sec": agg["duration_sec"],
                "success":      agg["success"],
                "failure":      agg["failure"],
                "other":        agg["other"],
                "listen":       agg["listen"],
            }

    # ── 3. Re-write the full merged CSV ──────────────────────────
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for (db_name, state, use_case, language, region, date_label, hour_label), agg in existing.items():
            total     = agg["total"]
            connected = agg["connected"]
            dur_sec   = agg["duration_sec"]
            success   = agg["success"]
            failure   = agg["failure"]
            other     = agg["other"]
            listen    = agg["listen"]

            connected_rate = round(connected / total * 100, 2) if total else 0
            dur_min        = round(dur_sec / 60, 4)
            dur_hr         = round(dur_sec / 3600, 4)
            success_rate   = round(success / total * 100, 2) if total else 0
            listen_rate    = round(listen / total * 100, 6) if total else 0

            writer.writerow([
                db_name, state, use_case, language, region,
                date_label, hour_label,
                total, connected, connected_rate,
                dur_sec, dur_min, dur_hr,
                success, failure, other,
                success_rate, listen, listen_rate,
            ])


# ================================================================
# STEP 8 — Main runner (called by cron every hour)
# ================================================================
def run_pipeline():
    run_start = datetime.now()
    print(f"\n{'='*60}")
    print(f"[{run_start.strftime('%Y-%m-%d %H:%M:%S')}] Pipeline started")
    print(f"{'='*60}")

    # ── Auto-discover databases ──────────────────────────────────
    if INCLUDE_DBS:
        db_names = INCLUDE_DBS
        print(f"  Using specified DBs ({len(db_names)}): {db_names}")
    else:
        _disc_client = MongoClient(MONGO_URI)
        try:
            all_db_names = _disc_client.list_database_names()
        except Exception as e:
            print(f"  ERROR listing databases: {e}")
            _disc_client.close()
            return
        finally:
            _disc_client.close()

        db_names = [
            d for d in all_db_names
            if d not in SYSTEM_DBS and d not in EXCLUDE_DBS
        ]
        print(f"  Auto-discovered {len(db_names)} DBs: {db_names}")

    checkpoints    = load_checkpoint()         # {db: {"last_time": ISO, "done_ids": [...]}}
    new_checkpoints = {db: dict(entry) for db, entry in checkpoints.items()}
    all_rows = {}
    now_utc  = datetime.now(timezone.utc).replace(tzinfo=None)   # single timestamp for this run

    # ── Worker: process one DB (each thread gets its own MongoClient) ──
    def process_one(db_name: str):
        entry        = checkpoints.get(db_name)          # None on first run
        is_first_run = entry is None
        if is_first_run:
            last_time = datetime.min
            done_ids  = set()
            print(f"  [{db_name}] First run — fetching ALL historical data")
        else:
            last_time = datetime.fromisoformat(entry["last_time"])
            done_ids  = set(entry.get("done_ids") or [])
            print(f"  [{db_name}] Incremental — lookback {LOOKBACK_HOURS}h | {len(done_ids)} IDs already processed")
        _client = MongoClient(MONGO_URI)
        try:
            rows, new_exec_ids = process_db(db_name, _client[db_name], done_ids, last_time, is_first_run)
            return db_name, rows, new_exec_ids, is_first_run, None
        except Exception as e:
            return db_name, {}, [], is_first_run, str(e)
        finally:
            _client.close()

    # ── Run all DBs in parallel (max 5 concurrent connections) ────────
    MAX_WORKERS = 5
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_one, name): name for name in db_names}
        for future in as_completed(futures):
            db_name, rows, new_exec_ids, is_first_run, err = future.result()
            if err:
                print(f"  [{db_name}] ERROR: {err}")
                continue
            all_rows.update(rows)

            # ── Update checkpoint entry for this DB ──────────────────
            prev_entry  = new_checkpoints.get(db_name) or {"last_time": now_utc.isoformat(), "done_ids": []}
            prev_done   = set(prev_entry.get("done_ids") or [])
            prev_done.update(str(i) for i in new_exec_ids)   # mark newly processed IDs

            if rows or not is_first_run:
                new_checkpoints[db_name] = {
                    "last_time": now_utc.isoformat(),
                    "done_ids":  list(prev_done),
                }
            else:
                print(f"  [{db_name}] First run returned 0 sessions — will retry on next run")

    if all_rows:
        write_to_csv(all_rows, OUTPUT_FILE)
        print(f"\n  Written {len(all_rows):,} rows to {OUTPUT_FILE}")
    else:
        print("\n  No new sessions found.")

    save_checkpoint(new_checkpoints)

    # Always rebuild dashboard JSON from the full CSV after each run
    write_dashboard_json(OUTPUT_FILE, "dashboard_data.json")

    elapsed = (datetime.now() - run_start).total_seconds()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Done in {elapsed:.1f}s\n")


# ================================================================
# STEP 9 — Build dashboard_data.json from full CSV
# ================================================================
def write_dashboard_json(csv_file: str, json_file: str):
    """
    Reads session_analytics_realtime.csv (all-time, cumulative) and writes
    dashboard_data.json which the HTML dashboard fetches every hour.
    """
    try:
        import pandas as pd
    except ImportError:
        print("  [JSON] pandas not installed — skipping dashboard JSON build")
        return

    if not os.path.exists(csv_file):
        print(f"  [JSON] CSV not found: {csv_file}")
        return

    df = pd.read_csv(csv_file, encoding='utf-8')
    df.columns = df.columns.str.strip()

    # ── Helpers ───────────────────────────────────────────────────
    def hour_sort_key(h):
        try:
            return datetime.strptime(str(h).strip(), "%I:%M:%S %p").hour
        except Exception:
            return 99

    def parse_date(d):
        for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(str(d).strip(), fmt)
            except Exception:
                pass
        return None

    def cap100(pos, conn):
        """Success rate = positive/connected capped at 100. Some calls (LM/CB)
        have a success disposition but duration <= threshold, so positive can
        slightly exceed connected — cap prevents >100% in the dashboard."""
        if not conn:
            return 0.0
        return min(100.0, round(pos / conn * 100, 2))

    # ── Normalise Language column case before groupby ─────────────
    # Ensures 'english' and 'English' merge into one bucket
    df["Language"] = df["Language"].fillna("unknown").astype(str).str.lower().str.strip()

    # ── KPIs ─────────────────────────────────────────────────────
    total_calls = int(df["Total_calls"].sum())
    connected   = int(df["Connected_calls"].sum())
    positive    = int(df["Positive disposition"].sum())
    negative    = int(df["Negative disposition"].sum())
    listen      = int(df["Listen Count"].sum())
    dur_sec_all = float(df["Total_duration_seconds"].sum())
    dur_hrs     = round(dur_sec_all / 3600, 2)
    conn_rate   = round(connected / total_calls * 100, 2) if total_calls else 0
    # Only count companies that actually have calls
    companies   = int((df.groupby("Company name")["Total_calls"].sum() > 0).sum())

    kpis = {
        "total_calls":     total_calls,
        "connected_calls": connected,
        "connect_rate":    conn_rate,
        "total_hours":     round(dur_hrs, 1),
        "positive":        positive,
        "negative":        negative,
        "listen_count":    listen,
        "companies_count": companies,
    }

    # ── Company aggregation ──────────────────────────────────────
    cg = df.groupby("Company name").agg(
        total    =("Total_calls",           "sum"),
        connected=("Connected_calls",        "sum"),
        positive =("Positive disposition",   "sum"),
        negative =("Negative disposition",   "sum"),
        listen   =("Listen Count",           "sum"),
        dur_sec  =("Total_duration_seconds", "sum"),
    ).reset_index()
    # Only include companies with actual data
    cg = cg[cg["total"] > 0]

    company_data = []
    for _, r in cg.iterrows():
        cp  = round(r.connected / r.total * 100, 2) if r.total else 0
        suc = cap100(r.positive, r.connected)
        mht = round(r.dur_sec / r.connected, 1) if r.connected else 0
        company_data.append({
            "name": r["Company name"], "total": int(r.total),
            "connected": int(r.connected), "positive": int(r.positive),
            "negative": int(r.negative), "success": suc, "mht": mht,
            "listen": int(r.listen), "connPct": cp,
            "durationH": round(r.dur_sec / 3600, 2),
        })

    # ── Region aggregation ──────────────────────────────────────
    rg = df[df["Region"].notna() & (df["Region"] != "UNKNOWN")].groupby("Region").agg(
        total    =("Total_calls",         "sum"),
        connected=("Connected_calls",      "sum"),
        positive =("Positive disposition", "sum"),
    ).reset_index()
    region_data = []
    for _, r in rg.iterrows():
        cp  = round(r.connected / r.total * 100, 2) if r.total else 0
        suc = cap100(r.positive, r.connected)
        region_data.append({"name": r["Region"], "total": int(r.total),
                             "connected": int(r.connected), "success": suc, "connPct": cp})

    # ── Use-case aggregation ─────────────────────────────────────
    ug = df[df["Use case wise"].notna() & (df["Use case wise"] != "UNKNOWN")].groupby("Use case wise").agg(
        total    =("Total_calls",         "sum"),
        connected=("Connected_calls",      "sum"),
        positive =("Positive disposition", "sum"),
    ).reset_index()
    usecase_data = []
    for _, r in ug.iterrows():
        suc = cap100(r.positive, r.connected)
        usecase_data.append({"name": r["Use case wise"], "total": int(r.total),
                              "connected": int(r.connected), "success": suc})

    # ── Language aggregation (grouped on lowercased column) ───────
    lg = df[df["Language"].notna() & (df["Language"] != "unknown")].groupby("Language").agg(
        total    =("Total_calls",         "sum"),
        connected=("Connected_calls",      "sum"),
        positive =("Positive disposition", "sum"),
    ).reset_index()
    language_data = []
    for _, r in lg.iterrows():
        suc = cap100(r.positive, r.connected)
        language_data.append({"name": r["Language"].title(), "total": int(r.total),
                               "connected": int(r.connected), "success": suc})

    # ── Date aggregation (sorted chronologically) ────────────────
    df["_dt"] = df["Date"].apply(parse_date)
    df_dated  = df.dropna(subset=["_dt"])
    dg = df_dated.groupby("Date").agg(
        total    =("Total_calls",         "sum"),
        connected=("Connected_calls",      "sum"),
        positive =("Positive disposition", "sum"),
        _dt      =("_dt",                  "first"),
    ).reset_index().sort_values("_dt")
    date_data = []
    for _, r in dg.iterrows():
        suc = cap100(r.positive, r.connected)
        date_data.append({"d": r["_dt"].strftime("%d %b %Y"),
                          "total": int(r.total), "connected": int(r.connected), "success": suc})

    # ── Hour aggregation (sorted by clock) ──────────────────────
    # Filter out junk hour values (non-time-format strings)
    df_hours = df[df["Hour"].notna() & df["Hour"].astype(str).str.match(r"^\d{1,2}:\d{2}:\d{2} [AP]M$")]
    hg = df_hours.groupby("Hour").agg(
        total    =("Total_calls",         "sum"),
        connected=("Connected_calls",      "sum"),
        positive =("Positive disposition", "sum"),
    ).reset_index()
    hg["_sort"] = hg["Hour"].apply(hour_sort_key)
    hg = hg.sort_values("_sort")
    hour_data = []
    for _, r in hg.iterrows():
        try:
            dt      = datetime.strptime(str(r["Hour"]).strip(), "%I:%M:%S %p")
            h_label = dt.strftime("%I:%M %p").lstrip("0") or "12:00 AM"
        except Exception:
            h_label = str(r["Hour"])
        suc = cap100(r.positive, r.connected)
        hour_data.append({"h": h_label, "total": int(r.total),
                          "connected": int(r.connected), "success": suc})

    # ── Per-company date & hour data ────────────────────────────
    comp_date_data = {}
    comp_hour_data = {}

    for company, cdf in df_dated.groupby("Company name"):
        # date
        cdg = cdf.groupby("Date").agg(
            total    =("Total_calls",           "sum"),
            connected=("Connected_calls",        "sum"),
            positive =("Positive disposition",   "sum"),
            dur_sec  =("Total_duration_seconds", "sum"),
            _dt      =("_dt",                    "first"),
        ).reset_index().sort_values("_dt")
        comp_date_data[company] = [
            {"d": r["_dt"].strftime("%d %b %Y"), "total": int(r.total),
             "connected": int(r.connected), "positive": int(r.positive),
             "durationH": round(r.dur_sec / 3600, 2)}
            for _, r in cdg.iterrows()
        ]
        # hour
        chg = cdf[cdf["Hour"].notna()].groupby("Hour").agg(
            total    =("Total_calls",           "sum"),
            connected=("Connected_calls",        "sum"),
            positive =("Positive disposition",   "sum"),
            dur_sec  =("Total_duration_seconds", "sum"),
        ).reset_index()
        chg["_sort"] = chg["Hour"].apply(hour_sort_key)
        chg = chg.sort_values("_sort")
        h_list = []
        for _, r in chg.iterrows():
            try:
                dt      = datetime.strptime(str(r["Hour"]).strip(), "%I:%M:%S %p")
                h_label = dt.strftime("%I:%M %p").lstrip("0") or "12:00 AM"
            except Exception:
                h_label = str(r["Hour"])
            h_list.append({"h": h_label, "total": int(r.total),
                           "connected": int(r.connected), "positive": int(r.positive),
                           "durationH": round(r.dur_sec / 3600, 2)})
        comp_hour_data[company] = h_list

    # ── Write JSON ───────────────────────────────────────────────
    dashboard = {
        "last_updated":  datetime.now(timezone.utc).astimezone(
            timezone(timedelta(hours=5, minutes=30))
        ).strftime("%Y-%m-%d %H:%M:%S IST"),
        "kpis":          kpis,
        "company":       company_data,
        "region":        region_data,
        "usecase":       usecase_data,
        "language":      language_data,
        "date":          date_data,
        "hour":          hour_data,
        "compDateData":  comp_date_data,
        "compHourData":  comp_hour_data,
    }
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)

    print(f"  [JSON] dashboard_data.json -> {companies} companies | "
          f"{len(date_data)} dates | {len(hour_data)} hours")


if __name__ == "__main__":
    run_pipeline()
