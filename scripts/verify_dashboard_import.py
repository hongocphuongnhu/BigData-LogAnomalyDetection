"""Import dashboard trong Kibana space moi, giu nguyen volume va dashboard chinh."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid

from stack_api import ROOT, request


def main() -> None:
    space = "nhu-qa-" + uuid.uuid4().hex[:8]
    request("POST", "/api/spaces/space", {"id": space, "name": "Nhu import verification", "disabledFeatures": []}, kibana=True)
    environment = dict(os.environ)
    environment["KIBANA_URL"] = "http://localhost:5601/s/" + space
    # Tuy chon API URL cua host duoc giu trong env_config, khong hardcode auth.
    flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
    subprocess.run([sys.executable, "-B", "scripts/kibana_import.py"], cwd=ROOT, env=environment, check=True, creationflags=flags)
    prefix = "/s/" + space
    try:
        dashboards = request("GET", prefix + "/api/saved_objects/_find?type=dashboard&per_page=100", kibana=True)["saved_objects"]
        assert len(dashboards) == 2
        references = sum(len(item["references"]) for item in dashboards)
        report = {"space": space, "dashboards": len(dashboards), "panel_references": references,
                  "verified_in_new_space": True, "volume_reset": False,
                  "url": f"http://localhost:5601/s/{space}/app/dashboards"}
        assert references == 18
        # Kibana co the doi ID khi import saved object sang space khac.
        lenses = request("GET", prefix + "/api/saved_objects/_find?type=lens&per_page=100", kibana=True)["saved_objects"]
        lens = next(item for item in lenses if item["attributes"]["title"] == "Ground truth vs prediction (simulator)")
        columns = lens["attributes"]["state"]["datasourceStates"]["formBased"]["layers"]["layer"]["columns"]
        assert columns["breakdown"]["sourceField"] == "is_alert"
    finally:
        # Ke ca assertion loi, khong de rule QA chay song song voi rule demo.
        rules = request("GET", prefix + "/api/alerting/rules/_find?per_page=100", kibana=True)["data"]
        for rule in rules:
            request("POST", prefix + f"/api/alerting/rule/{rule['id']}/_disable", kibana=True)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
