"""
landing.py – Write one API page as one NDJSON file into the landing zone, where Auto Loader picks it up.

    LocalLanding     a folder on this machine (local runs: data/shipments/ or a temp folder)
    DbutilsLanding   a cloud path on Databricks (abfss://landing@<account>.dfs.core.windows.net/shipments),
                     written with dbutils.fs.put: plain open() cannot write to abfss://

One line per record, exactly as the API returned it, so the Bronze contract of the feed is unchanged.
"""

import json
import os
from pathlib import Path
from typing import Any, List


def to_ndjson(records: List[dict]) -> str:
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records)


class LocalLanding:
    def __init__(self, folder: Path):
        self.folder = Path(folder)

    def write(self, name: str, records: List[dict]) -> str:
        self.folder.mkdir(parents=True, exist_ok=True)
        target = self.folder / name
        tmp = target.with_name(name + ".tmp")          # not *.jsonl, so a reader never sees half a file
        tmp.write_text(to_ndjson(records), encoding="utf-8")
        os.replace(tmp, target)
        return str(target)


class DbutilsLanding:
    def __init__(self, base_path: str, dbutils: Any):
        self.base_path, self.dbutils = base_path.rstrip("/"), dbutils

    def write(self, name: str, records: List[dict]) -> str:
        target = f"{self.base_path}/{name}"
        self.dbutils.fs.put(target, to_ndjson(records), True)       # overwrite: a repaired run rewrites its own pages
        return target
