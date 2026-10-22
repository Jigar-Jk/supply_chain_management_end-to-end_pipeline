"""
apply_migrations.py – Apply the Liquibase changelog to Databricks catalogs through a SQL warehouse.

Same files, same order as liquibase/changelog/db.changelog-master.xml: the table changesets
(changes/001-015), then the Gold views and mart. Each statement is sent to the SQL Statement API
with the target catalog as default, so one run creates identical tables in dev, qa and prod.

Stand-in for the Liquibase CLI (not installed): it does not keep Liquibase's DATABASECHANGELOG
history, which is fine because every session starts from freshly created, empty catalogs.

Login: an Azure AD token from your `az login` (Azure Databricks), or a Databricks CLI profile.

Usage (scripts/up.ps1 runs this for you):
    python scripts/apply_migrations.py --host https://adb-....azuredatabricks.net --warehouse-id <id> \
        --catalog scm_platform_dev --catalog scm_platform_qa --catalog scm_platform_prod
    python scripts/apply_migrations.py --profile scm-free --host https://dbc-... --warehouse-id <id> --catalog scm_platform_free
"""

import argparse
import json
import re
import subprocess
import sys
import time
import xml.dom.minidom
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent
CHANGELOG = REPO / "liquibase" / "changelog"
AZURE_DATABRICKS_APP_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"   # resource id for Azure AD tokens


def access_token(profile: str | None) -> str:
    """A token for the Databricks REST API; never printed."""
    if profile:
        out = subprocess.run(["databricks", "auth", "token", "-p", profile], capture_output=True, text=True, check=True)
        return json.loads(out.stdout)["access_token"]
    out = subprocess.run(["az", "account", "get-access-token", "--resource", AZURE_DATABRICKS_APP_ID,
                          "--query", "accessToken", "-o", "tsv"], capture_output=True, text=True, check=True, shell=True)
    return out.stdout.strip()


def changelog_files() -> list[Path]:
    """The SQL files in the order the master changelog applies them."""
    doc = xml.dom.minidom.parse(str(CHANGELOG / "db.changelog-master.xml"))
    files = [CHANGELOG / n.getAttribute("file") for n in doc.getElementsByTagName("include")]
    return files + [CHANGELOG / n.getAttribute("path") for n in doc.getElementsByTagName("sqlFile")]


def statements(path: Path) -> list[str]:
    """Split a Liquibase-formatted SQL file into statements (comment lines dropped)."""
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if not ln.lstrip().startswith("--")]
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", "\n".join(lines)) if s.strip()]


def run_statement(session: requests.Session, host: str, warehouse: str, catalog: str, sql: str) -> tuple[str, str]:
    r = session.post(f"{host}/api/2.0/sql/statements", timeout=120, json={
        "warehouse_id": warehouse, "statement": sql, "catalog": catalog,
        "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
    r.raise_for_status()
    body = r.json()
    while body["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        body = session.get(f"{host}/api/2.0/sql/statements/{body['statement_id']}", timeout=60).json()
    return body["status"]["state"], body["status"].get("error", {}).get("message", "")


def apply(session: requests.Session, host: str, warehouse: str, catalog: str) -> int:
    """Apply every changelog file to one catalog; returns the number of failed statements."""
    failures = 0
    for f in changelog_files():
        stmts = statements(f)
        bad = []
        for sql in stmts:
            state, err = run_statement(session, host, warehouse, catalog, sql)
            if state != "SUCCEEDED":
                bad.append((sql.split("\n")[0][:90], err.splitlines()[0][:220] if err else state))
        failures += len(bad)
        print(f"  {'OK  ' if not bad else 'FAIL'} {f.relative_to(CHANGELOG).as_posix()}  ({len(stmts) - len(bad)}/{len(stmts)})")
        for first, err in bad:
            print(f"         ! {first}\n           {err}")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply the Liquibase changelog to Databricks catalogs.")
    parser.add_argument("--host", required=True, help="Workspace URL (terraform output databricks_workspace_url)")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse id (terraform output sql_warehouse_id)")
    parser.add_argument("--catalog", action="append", required=True, help="Target catalog; repeat for several")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile instead of the az login token")
    args = parser.parse_args()

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {access_token(args.profile)}"
    host = args.host.rstrip("/")
    total = 0
    for catalog in args.catalog:
        print(f"===== {catalog}")
        failed = apply(session, host, args.warehouse_id, catalog)
        print(f"  {failed} failed statement(s)")
        total += failed
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
