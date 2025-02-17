#!/usr/bin/env python
from pathlib import Path
import json

static = Path(__file__).parent / "static" 
logs = static / "logs"
manifest = static / "manifest.json"
# get a list of all the job ids in the current job directory

reports = list(logs.glob("Report-*.out"))
job_ids = sorted([int(report.name.replace(".out", "").split("-")[1]) for report in reports])
data = json.dumps(job_ids)
manifest.write_text(data)
print(data)
