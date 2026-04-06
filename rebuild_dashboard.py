"""
rebuild_dashboard.py
Standalone script — imports write_dashboard_json from pipeline_v2 and
regenerates dashboard_data.json from the existing CSV.
Run once: python rebuild_dashboard.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from pipeline_v2 import write_dashboard_json

CSV  = os.path.join(os.path.dirname(__file__), "session_analytics_realtime.csv")
JSON = os.path.join(os.path.dirname(__file__), "dashboard_data.json")

print("Rebuilding dashboard_data.json ...")
write_dashboard_json(CSV, JSON)
print("Done.")
