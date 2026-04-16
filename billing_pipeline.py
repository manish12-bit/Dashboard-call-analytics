#!/usr/bin/env python3
"""
Billing Pipeline — nightly incremental call & SMS billing aggregation.
Runs at 22:00 IST (16:30 UTC) via GitHub Actions.

Strategy
--------
- `billing_daily_cache.json`  stores per-company, per-date daily totals
  (call billing AND sms billing) so old data is never re-fetched.
- `billing_checkpoint.json`   stores last-processed date per company
  for both call and sms billing.
- Each nightly run only fetches dates AFTER the last checkpoint date,
  merges into the cache, then recalculates all period windows.
- Output `billing_data.json` is read by the dashboard frontend.
"""

import os
import json
import logging
from datetime import datetime, timedelta, date
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Configuration ─────────────────────────────────────────────────────────────

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://wordworksai:GRXtnwKRM4qazkkF@"
    "wwai-prod-main-stable-db-1.global.mongocluster.cosmos.azure.com/",
)

BILLING_COLL     = "billing"
SMS_BILLING_COLL = "sms_billing"
CHECKPOINT_FILE  = "billing_checkpoint.json"
DAILY_CACHE_FILE = "billing_daily_cache.json"
OUTPUT_JSON      = "billing_data.json"

COMPANY_MAP = {
    "agrim":               "Agrim",
    "bajaj":               "Bajaj",
    "cred":                "Cred",
    "dbs_mintek":          "DBS Mintek",
    "dubai_expats":        "Dubai Expats",
    "ecofy":               "Ecofy",
    "hdfc":                "HDFC",
    "housing":             "Housing",
    "inprime":             "Inprime",
    "it_cart":             "IT Cart",
    "kissht":              "Kissht",
    "micro_fi":            "Microfinance",
    "mktg_micro_fi":       "Mktg Microfinance",
    "multify":             "Multify",
    "opus":                "Opus",
    "salesbot_datasheets": "Salesbot",
    "shubham_housing":     "Shubham Housing",
    "smart_dial":          "Smart Dial",
    "tatacapital":         "Tata Capital",
    "ugro":                "Ugro",
}

INCLUDE_DBS = list(COMPANY_MAP.keys())

# ─── Date & Fiscal-Year Helpers ────────────────────────────────────────────────

def today_ist() -> date:
    """Today's date in IST (UTC+5:30)."""
    return (datetime.now(tz=None) + timedelta(hours=5, minutes=30)).date()


def billing_reference_date() -> date:
    """
    Reference date used for billing period windows.

    The DB is updated with billing data at ~19:00 IST every day.
    If the pipeline runs before 19:00 IST (e.g. the nightly job is
    delayed past midnight IST by GitHub Actions), today_ist() would
    return tomorrow's date while the DB still has no data for it —
    causing the 'today' KPI to show Nil until the next nightly run.

    Rule: before 19:00 IST, treat yesterday as 'today' for billing.
    """
    ist_now = datetime.now(tz=None) + timedelta(hours=5, minutes=30)
    if ist_now.hour < 19:          # DB hasn't received today's data yet
        return (ist_now - timedelta(days=1)).date()
    return ist_now.date()


def week_range(d: date):
    monday = d - timedelta(days=d.weekday())
    return monday, monday + timedelta(days=6)


def fiscal_year_of(d: date):
    """Fiscal year April 1 – March 31."""
    if d.month >= 4:
        return date(d.year, 4, 1), date(d.year + 1, 3, 31)
    return date(d.year - 1, 4, 1), date(d.year, 3, 31)


def compute_periods(today: date) -> dict:
    w_start, w_end = week_range(today)
    lw_start = w_start - timedelta(days=7)
    lw_end   = lw_start + timedelta(days=6)
    cm_start = today.replace(day=1)
    lm_end   = cm_start - timedelta(days=1)
    lm_start = lm_end.replace(day=1)
    same_day_num  = min(today.day, lm_end.day)
    lm_same_date  = lm_start.replace(day=same_day_num)
    lm_same_day   = lm_same_date
    fy_start, fy_end   = fiscal_year_of(today)
    pfy_start = date(fy_start.year - 1, fy_start.month, fy_start.day)
    pfy_end   = date(fy_end.year - 1,   fy_end.month,   fy_end.day)
    return {
        "today":                 (today,        today),
        "this_week":             (w_start,      today),
        "last_week":             (lw_start,     lw_end),
        "current_month":         (cm_start,     today),
        "last_month_same_date":  (lm_start,     lm_same_date),
        "last_month_total":      (lm_start,     lm_end),
        "last_month_same_day":   (lm_same_day,  lm_same_day),
        "this_year":             (fy_start,     today),
        "last_year":             (pfy_start,    pfy_end),
    }


def date_gt_query(last_date: date) -> dict:
    """MongoDB query returning docs where year/month/day > last_date."""
    y, m, d = last_date.year, last_date.month, last_date.day
    return {"$or": [
        {"year": {"$gt": y}},
        {"year": y, "month": {"$gt": m}},
        {"year": y, "month": m, "day": {"$gt": d}},
    ]}


def doc_to_date(doc) -> date | None:
    try:
        return date(int(doc["year"]), int(doc["month"]), int(doc["day"]))
    except Exception:
        return None


# ─── Checkpoint & Cache I/O ────────────────────────────────────────────────────

def load_json(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ─── MongoDB ───────────────────────────────────────────────────────────────────

def get_client() -> MongoClient:
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=15_000,
        connectTimeoutMS=15_000,
        socketTimeoutMS=120_000,
        retryReads=True,
        retryWrites=True,
    )


# ─── Fetch new docs from MongoDB ───────────────────────────────────────────────

def fetch_new_call_docs(db_name: str, client: MongoClient, since: date | None) -> list:
    """
    Fetch call billing docs newer than `since` date.
    If since is None (first run) fetches everything.
    """
    try:
        query = date_gt_query(since) if since else {}
        docs = list(
            client[db_name][BILLING_COLL].find(
                query,
                {"year": 1, "month": 1, "day": 1, "stats": 1},
            )
        )
        log.info("  [%s] call billing: %d new docs (since %s)", db_name, len(docs), since)
        return docs
    except Exception as exc:
        log.warning("  [%s] call billing error: %s", db_name, exc)
        return []


def fetch_new_sms_docs(db_name: str, client: MongoClient, since: date | None) -> list:
    """
    Fetch SMS billing docs newer than `since` date.
    SMS billing trigger basis: date field (each doc = one day's SMS activity).
    """
    try:
        query = date_gt_query(since) if since else {}
        docs = list(
            client[db_name][SMS_BILLING_COLL].find(
                query,
                {"year": 1, "month": 1, "day": 1,
                 "summary": 1, "total_cost": 1, "total_production_cost": 1},
            )
        )
        log.info("  [%s] sms billing: %d new docs (since %s)", db_name, len(docs), since)
        return docs
    except Exception as exc:
        log.warning("  [%s] sms billing error: %s", db_name, exc)
        return []


# ─── Merge new docs into daily cache ──────────────────────────────────────────

def merge_call_docs(db_cache: dict, docs: list) -> tuple[dict, date | None]:
    """
    Merge new call billing docs into the per-company call cache.
    Returns (updated_cache, latest_date_seen).
    Cache structure: {"2026-01-14": {"calls": N, "cost": F, "duration_sec": N}}
    """
    latest = None
    for doc in docs:
        d = doc_to_date(doc)
        if d is None:
            continue
        s    = doc.get("stats", {})
        key  = d.isoformat()
        # If same date already in cache, accumulate (shouldn't happen — one doc per day)
        prev = db_cache.get(key, {"calls": 0, "cost": 0.0, "duration_sec": 0})
        db_cache[key] = {
            "calls":        prev["calls"]        + int(s.get("total_calls", 0)),
            "cost":         prev["cost"]         + float(s.get("total_cost", 0)),
            "duration_sec": prev["duration_sec"] + int(s.get("total_duration_seconds", 0)),
        }
        if latest is None or d > latest:
            latest = d
    return db_cache, latest


def merge_sms_docs(db_cache: dict, docs: list) -> tuple[dict, date | None]:
    """
    Merge new SMS billing docs into per-company sms cache.
    Cache structure: {"2026-03-28": {"cost": F, "sms_count": N, "sms_units": N}}
    """
    latest = None
    for doc in docs:
        d = doc_to_date(doc)
        if d is None:
            continue
        cost  = float(doc.get("total_cost", doc.get("total_production_cost", 0)))
        summ  = doc.get("summary", {})
        count = int(summ.get("total_sms_count", 0))
        units = int(summ.get("total_sms_units", 0))
        key   = d.isoformat()
        prev  = db_cache.get(key, {"cost": 0.0, "sms_count": 0, "sms_units": 0})
        db_cache[key] = {
            "cost":      prev["cost"]      + cost,
            "sms_count": prev["sms_count"] + count,
            "sms_units": prev["sms_units"] + units,
        }
        if latest is None or d > latest:
            latest = d
    return db_cache, latest


# ─── Period aggregation from cache ────────────────────────────────────────────

def sum_call_period(call_cache: dict, start: date, end: date) -> dict:
    calls, cost, dur = 0, 0.0, 0
    for date_str, data in call_cache.items():
        try:
            d = date.fromisoformat(date_str)
        except ValueError:
            continue
        if start <= d <= end:
            calls += data.get("calls", 0)
            cost  += data.get("cost", 0)
            dur   += data.get("duration_sec", 0)
    return {"calls": calls, "cost": round(cost, 2), "duration_sec": dur}


def sum_sms_period(sms_cache: dict, start: date, end: date) -> dict:
    cost, count, units = 0.0, 0, 0
    for date_str, data in sms_cache.items():
        try:
            d = date.fromisoformat(date_str)
        except ValueError:
            continue
        if start <= d <= end:
            cost  += data.get("cost", 0)
            count += data.get("sms_count", 0)
            units += data.get("sms_units", 0)
    return {"cost": round(cost, 2), "sms_count": count, "sms_units": units}


# ─── Grand totals ──────────────────────────────────────────────────────────────

def compute_totals(companies: list, periods: dict) -> dict:
    totals = {
        p: {"call_cost": 0.0, "call_calls": 0, "sms_cost": 0.0, "sms_count": 0}
        for p in periods
    }
    for comp in companies:
        for p in periods:
            c = comp["call"].get(p, {})
            s = comp["sms"].get(p, {})
            totals[p]["call_cost"]  += c.get("cost", 0)
            totals[p]["call_calls"] += c.get("calls", 0)
            totals[p]["sms_cost"]   += s.get("cost", 0)
            totals[p]["sms_count"]  += s.get("sms_count", 0)
    for p in totals:
        totals[p]["call_cost"] = round(totals[p]["call_cost"], 2)
        totals[p]["sms_cost"]  = round(totals[p]["sms_cost"],  2)
    return totals


# ─── Main pipeline ─────────────────────────────────────────────────────────────

def run_billing_pipeline():
    log.info("=" * 55)
    log.info("Billing Pipeline START  (incremental mode)")
    log.info("=" * 55)

    today   = billing_reference_date()
    periods = compute_periods(today)

    log.info("Reference date (IST): %s  [IST now: %s]",
             today,
             (datetime.now(tz=None) + timedelta(hours=5, minutes=30)).strftime("%H:%M"))
    for name, (s, e) in periods.items():
        log.info("  %-28s  %s  →  %s", name, s, e)

    # Load existing checkpoint and daily cache
    cp    = load_json(CHECKPOINT_FILE)
    cache = load_json(DAILY_CACHE_FILE)

    client    = get_client()
    companies = []

    for db_name in INCLUDE_DBS:
        log.info("[%s] Processing ...", db_name)

        # Per-company cache (call + sms dicts keyed by ISO date string)
        db_entry = cache.get(db_name, {"call": {}, "sms": {}})
        db_call  = db_entry.get("call", {})
        db_sms   = db_entry.get("sms",  {})

        # Last processed dates
        cp_entry       = cp.get(db_name, {})
        last_call_str  = cp_entry.get("call_last_date")
        last_sms_str   = cp_entry.get("sms_last_date")
        last_call_date = date.fromisoformat(last_call_str) if last_call_str else None
        last_sms_date  = date.fromisoformat(last_sms_str)  if last_sms_str  else None

        # Re-fetch last 3 days to catch late-arriving billing data
        refetch_call = (last_call_date - timedelta(days=3)) if last_call_date else None
        refetch_sms  = (last_sms_date  - timedelta(days=3)) if last_sms_date  else None

        # Clear re-fetch overlap from cache before re-merge (prevents double-counting)
        if refetch_call:
            for k in list(db_call.keys()):
                try:
                    if date.fromisoformat(k) > refetch_call:
                        del db_call[k]
                except ValueError:
                    pass
        if refetch_sms:
            for k in list(db_sms.keys()):
                try:
                    if date.fromisoformat(k) > refetch_sms:
                        del db_sms[k]
                except ValueError:
                    pass

        try:
            # Fetch docs with 3-day re-check overlap for late-arriving data
            call_docs = fetch_new_call_docs(db_name, client, refetch_call)
            sms_docs  = fetch_new_sms_docs(db_name, client, refetch_sms)

            # Merge into cache
            db_call, new_call_latest = merge_call_docs(db_call, call_docs)
            db_sms,  new_sms_latest  = merge_sms_docs(db_sms,  sms_docs)

            # Update cache entry
            cache[db_name] = {"call": db_call, "sms": db_sms}

            # Update checkpoint with latest dates seen
            new_cp = dict(cp_entry)
            if new_call_latest:
                # Keep whichever is newer: existing checkpoint or new docs
                existing_call = date.fromisoformat(last_call_str) if last_call_str else None
                best_call     = max(new_call_latest, existing_call) if existing_call else new_call_latest
                new_cp["call_last_date"] = best_call.isoformat()
            if new_sms_latest:
                existing_sms = date.fromisoformat(last_sms_str) if last_sms_str else None
                best_sms     = max(new_sms_latest, existing_sms) if existing_sms else new_sms_latest
                new_cp["sms_last_date"] = best_sms.isoformat()
            new_cp["last_run"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            new_cp["call_days_cached"] = len(db_call)
            new_cp["sms_days_cached"]  = len(db_sms)
            cp[db_name] = new_cp

            # Build period aggregates from the FULL cache (old + new)
            call_periods = {p: sum_call_period(db_call, s, e) for p, (s, e) in periods.items()}
            sms_periods  = {p: sum_sms_period(db_sms,  s, e) for p, (s, e) in periods.items()}

            # Latest data dates
            call_dates = sorted(date.fromisoformat(k) for k in db_call if db_call[k].get("calls", 0) > 0 or db_call[k].get("cost", 0) > 0)
            sms_dates  = sorted(date.fromisoformat(k) for k in db_sms  if db_sms[k].get("sms_count", 0) > 0 or db_sms[k].get("cost", 0) > 0)

            companies.append({
                "name":              COMPANY_MAP[db_name],
                "db":                db_name,
                "call":              call_periods,
                "sms":               sms_periods,
                "latest_call_date":  call_dates[-1].isoformat() if call_dates else None,
                "latest_sms_date":   sms_dates[-1].isoformat()  if sms_dates  else None,
                "call_days_cached":  len(db_call),
                "sms_days_cached":   len(db_sms),
            })

        except Exception as exc:
            log.error("[%s] Failed: %s", db_name, exc)
            # Still add the company with whatever is in cache
            call_periods = {p: sum_call_period(db_call, s, e) for p, (s, e) in periods.items()}
            sms_periods  = {p: sum_sms_period(db_sms,  s, e) for p, (s, e) in periods.items()}
            companies.append({
                "name":  COMPANY_MAP[db_name], "db": db_name,
                "call":  call_periods, "sms": sms_periods,
                "latest_call_date": None, "latest_sms_date": None,
                "call_days_cached": len(db_call), "sms_days_cached": len(db_sms),
            })

    client.close()

    # Save updated cache & checkpoint
    save_json(DAILY_CACHE_FILE, cache)
    save_json(CHECKPOINT_FILE, cp)

    # Build output JSON
    totals   = compute_totals(companies, periods)
    fy_start, fy_end = fiscal_year_of(today)

    period_meta = {k: {"start": str(v[0]), "end": str(v[1])} for k, v in periods.items()}

    # Build daily_data (last 2 fiscal years) for date-range filtering in the dashboard
    data_cutoff_iso = date(fy_start.year - 1, 4, 1).isoformat()
    daily_data_out = {}
    for comp in companies:
        db = comp["db"]
        db_entry = cache.get(db, {"call": {}, "sms": {}})
        raw_call = db_entry.get("call", {})
        raw_sms  = db_entry.get("sms",  {})
        daily_data_out[db] = {
            "name": COMPANY_MAP[db],
            "call": {k: v for k, v in raw_call.items() if k >= data_cutoff_iso},
            "sms":  {k: v for k, v in raw_sms.items()  if k >= data_cutoff_iso},
        }

    output = {
        "last_updated":   datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
        "reference_date": str(today),
        "fiscal_year": {
            "this_year_label": f"Apr {fy_start.year} – Mar {fy_end.year}",
            "last_year_label": f"Apr {fy_start.year - 1} – Mar {fy_start.year}",
        },
        "period_meta":  period_meta,
        "totals":       totals,
        "companies":    companies,
        "daily_data":   daily_data_out,
    }

    save_json(OUTPUT_JSON, output)
    log.info("Written %s  (%d companies)", OUTPUT_JSON, len(companies))
    log.info("Daily cache saved to %s", DAILY_CACHE_FILE)
    log.info("Billing Pipeline DONE")


if __name__ == "__main__":
    run_billing_pipeline()
