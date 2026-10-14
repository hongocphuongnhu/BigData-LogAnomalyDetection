"""Import dashboard vao Kibana va tao lai connector/rule (khong export secrets)."""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path

from kibana_setup import setup_rule
from stack_api import ROOT, request


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, default=ROOT / "kibana" / "dashboards.ndjson")
    args = parser.parse_args()
    boundary = "log-anomaly-" + uuid.uuid4().hex
    data = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="dashboards.ndjson"\r\nContent-Type: application/ndjson\r\n\r\n'.encode()
            + args.file.read_bytes() + f"\r\n--{boundary}--\r\n".encode())
    result = request("POST", "/api/saved_objects/_import?overwrite=true", data, kibana=True, content_type=f"multipart/form-data; boundary={boundary}")
    if not result.get("success"):
        raise RuntimeError(f"Dashboard import failed: {result}")
    setup_rule()
    print(f"KIBANA IMPORT OK: {result['successCount']} objects")


if __name__ == "__main__":
    main()
