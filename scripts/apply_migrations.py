"""
apply_migrations.py – Apply the Liquibase changelog to Databricks catalogs through a SQL warehouse.

Same changesets, same order as liquibase/changelog/db.changelog-master.xml: the table changesets
(changes/*.sql, one per "-- changeset author:id" marker), then the Gold views and mart (one per
<changeSet> in the XML). Each statement is sent to the SQL Statement API with the target catalog as
default, so one run creates identical tables in dev, qa and prod.

Stand-in for the Liquibase CLI (not installed), with the part of Liquibase that makes re-running safe:
every catalog keeps a register, ops.schema_changelog (Liquibase's DATABASECHANGELOG), with one row per
applied changeset and the checksum of its SQL. On each run, per changeset:

    not in the register                        RUN, then recorded
    in the register, same checksum             SKIP
    changed, runOnChange (Gold views)          RE-RUN (CREATE OR REPLACE), checksum updated
    changed, table changeset                   EDITED: nothing is run - an applied changeset was edited;
                                               write a new changeset instead
A failed statement stops the catalog: later changesets usually build on earlier ones. Databricks has
no multi-statement transactions, so a changeset that fails half-way runs again in full next time
(changesets are small and mostly IF NOT EXISTS / CREATE OR REPLACE).

Login, first match wins (the token is never printed):
    --profile                                         a Databricks CLI profile
    DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET   OAuth as a service principal (CD, GitHub Actions)
    otherwise                                         an Azure AD token from your `az login`

Usage (scripts/up.ps1 runs this for you):
    python scripts/apply_migrations.py --host https://adb-....azuredatabricks.net --warehouse-id <id> \
        --catalog scm_platform_dev --catalog scm_platform_qa --catalog scm_platform_prod
    python scripts/apply_migrations.py ... --dry-run      # the plan only: what would run, nothing changes
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import xml.dom.minidom
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import requests

REPO = Path(__file__).resolve().parent.parent
CHANGELOG = REPO / "liquibase" / "changelog"
AZURE_DATABRICKS_APP_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"   # resource id for Azure AD tokens
REGISTER = "ops.schema_changelog"
CHANGESET_MARKER = re.compile(r"^-- changeset (\S+)\s*$")


@dataclass(frozen=True)
class Changeset:
    id: str                 # author:id, as Liquibase identifies it
    file: str               # relative to liquibase/changelog/
    statements: tuple
    run_on_change: bool

    @property
    def checksum(self) -> str:
        """SHA-256 of the SQL only: comments, line endings and trailing spaces do not count."""
        return hashlib.sha256("\n;\n".join(self.statements).encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ reading the changelog

def sql_lines(text: str) -> list[str]:
    return [ln.rstrip() for ln in text.replace("\r\n", "\n").split("\n") if not ln.lstrip().startswith("--")]


def split_statements(text: str) -> tuple:
    """Statements of a SQL text, comment lines dropped."""
    return tuple(s.strip() for s in re.split(r";\s*(?:\n|$)", "\n".join(sql_lines(text))) if s.strip())


def formatted_sql_changesets(path: Path) -> list[Changeset]:
    """One Changeset per '-- changeset author:id' marker; the header before the first marker is ignored."""
    found, current, body = [], None, []
    for line in path.read_text(encoding="utf-8").replace("\r\n", "\n").split("\n") + ["-- changeset END:END"]:
        m = CHANGESET_MARKER.match(line)
        if not m:
            body.append(line)
            continue
        if current:
            found.append(Changeset(current, path.relative_to(CHANGELOG).as_posix(),
                                   split_statements("\n".join(body)), run_on_change=False))
        current, body = m.group(1), []
    return found


def changesets() -> list[Changeset]:
    """Every changeset in the order the master changelog applies them."""
    doc = xml.dom.minidom.parse(str(CHANGELOG / "db.changelog-master.xml"))
    out = []
    for node in doc.documentElement.childNodes:
        if node.nodeName == "include":
            out += formatted_sql_changesets(CHANGELOG / node.getAttribute("file"))
        elif node.nodeName == "changeSet":
            sql_file = node.getElementsByTagName("sqlFile")[0]
            text = (CHANGELOG / sql_file.getAttribute("path")).read_text(encoding="utf-8")
            stmts = (split_statements(text) if sql_file.getAttribute("splitStatements") != "false"
                     else ("\n".join(sql_lines(text)).strip().rstrip(";").strip(),))
            out.append(Changeset(f"{node.getAttribute('author')}:{node.getAttribute('id')}",
                                 sql_file.getAttribute("path"), stmts,
                                 run_on_change=node.getAttribute("runOnChange") == "true"))
    ids = [c.id for c in out]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise ValueError(f"changeset ids used twice: {duplicates}")
    return out


# ------------------------------------------------------------------ deciding and applying

RUN, SKIP, RERUN, EDITED = "RUN", "SKIP", "RE-RUN", "EDITED"


def plan(all_changesets: list[Changeset], applied: dict) -> list[tuple]:
    """[(changeset, action)] given the register {changeset id: checksum}."""
    out = []
    for c in all_changesets:
        if c.id not in applied:
            out.append((c, RUN))
        elif applied[c.id] == c.checksum:
            out.append((c, SKIP))
        else:
            out.append((c, RERUN if c.run_on_change else EDITED))
    return out


def migrate(steps: list[tuple], execute: Callable[[str], Optional[str]], record: Callable[[Changeset], None],
            dry_run: bool = False) -> int:
    """Carry out a plan; execute(sql) returns an error message or None. Returns the number of failures."""
    counts = {a: sum(1 for _, x in steps if x == a) for a in (RUN, RERUN, SKIP, EDITED)}
    for c, action in steps:
        if action != SKIP:
            print(f"  {'would ' if dry_run else ''}{action:<7} {c.file}  {c.id}")
    if counts[EDITED]:
        print(f"  STOP: {counts[EDITED]} applied changeset(s) were edited - write a new changeset instead. "
              "Nothing was run.")
        return counts[EDITED]
    if not dry_run:
        for c, action in steps:
            if action == SKIP:
                continue
            for sql in c.statements:
                error = execute(sql)
                if error:
                    print(f"  FAIL    {c.id}\n          ! {sql.splitlines()[0][:90]}\n          {error[:220]}")
                    print("  Stopped: later changesets may depend on this one.")
                    return 1
            record(c)
    print(f"  {'plan: ' if dry_run else ''}run {counts[RUN]}, re-run {counts[RERUN]}, skipped {counts[SKIP]}")
    return 0


# ------------------------------------------------------------------ Databricks SQL Statement API

def access_token(host: str, profile: Optional[str]) -> str:
    """A token for the Databricks REST API; never printed."""
    if profile:
        out = subprocess.run(["databricks", "auth", "token", "-p", profile], capture_output=True, text=True, check=True)
        return json.loads(out.stdout)["access_token"]
    client_id, client_secret = os.environ.get("DATABRICKS_CLIENT_ID"), os.environ.get("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        r = requests.post(f"{host}/oidc/v1/token", auth=(client_id, client_secret), timeout=60,
                          data={"grant_type": "client_credentials", "scope": "all-apis"})
        if r.status_code != 200:
            raise SystemExit(f"OAuth login as {client_id} failed: HTTP {r.status_code}")
        return r.json()["access_token"]
    out = subprocess.run(["az", "account", "get-access-token", "--resource", AZURE_DATABRICKS_APP_ID,
                          "--query", "accessToken", "-o", "tsv"], capture_output=True, text=True, check=True, shell=True)
    return out.stdout.strip()


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class Warehouse:
    def __init__(self, session: requests.Session, host: str, warehouse: str, catalog: str):
        self.session, self.host, self.warehouse, self.catalog = session, host, warehouse, catalog

    def run(self, sql: str) -> dict:
        r = self.session.post(f"{self.host}/api/2.0/sql/statements", timeout=120, json={
            "warehouse_id": self.warehouse, "statement": sql, "catalog": self.catalog,
            "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
        r.raise_for_status()
        body = r.json()
        while body["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(2)
            body = self.session.get(f"{self.host}/api/2.0/sql/statements/{body['statement_id']}", timeout=60).json()
        return body

    def execute(self, sql: str) -> Optional[str]:
        body = self.run(sql)
        if body["status"]["state"] == "SUCCEEDED":
            return None
        return body["status"].get("error", {}).get("message", "") or body["status"]["state"]

    def applied(self, create: bool) -> dict:
        """{changeset id: checksum} from the register; empty if it does not exist yet."""
        if create:
            error = self.execute(
                f"CREATE TABLE IF NOT EXISTS {REGISTER} (changeset_id STRING NOT NULL, file STRING NOT NULL, "
                "checksum STRING NOT NULL, applied_at TIMESTAMP NOT NULL, applied_by STRING NOT NULL) "
                "COMMENT 'Applied Liquibase changesets (scripts/apply_migrations.py); never edit by hand'")
            if error:
                raise SystemExit(f"cannot create {REGISTER}: {error}")
        body = self.run(f"SELECT changeset_id, checksum FROM {REGISTER}")
        if body["status"]["state"] != "SUCCEEDED":
            if "TABLE_OR_VIEW_NOT_FOUND" in body["status"].get("error", {}).get("message", ""):
                return {}
            raise SystemExit(f"cannot read {REGISTER}: {body['status']}")
        return {row[0]: row[1] for row in (body.get("result") or {}).get("data_array") or []}

    def record(self, c: Changeset) -> None:
        error = self.execute(
            f"MERGE INTO {REGISTER} t USING (SELECT {sql_string(c.id)} AS changeset_id, {sql_string(c.file)} AS file, "
            f"{sql_string(c.checksum)} AS checksum) s ON t.changeset_id = s.changeset_id "
            "WHEN MATCHED THEN UPDATE SET file = s.file, checksum = s.checksum, applied_at = current_timestamp(), "
            "applied_by = current_user() "
            "WHEN NOT MATCHED THEN INSERT (changeset_id, file, checksum, applied_at, applied_by) "
            "VALUES (s.changeset_id, s.file, s.checksum, current_timestamp(), current_user())")
        if error:
            raise SystemExit(f"{c.id} was applied but could not be recorded: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply the Liquibase changelog to Databricks catalogs.")
    parser.add_argument("--host", required=True, help="Workspace URL (terraform output databricks_workspace_url)")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse id (terraform output sql_warehouse_id)")
    parser.add_argument("--catalog", action="append", required=True, help="Target catalog; repeat for several")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile instead of the az login token")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan only; change nothing")
    args = parser.parse_args()

    host = args.host.rstrip("/")
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {access_token(host, args.profile)}"
    all_changesets = changesets()
    total = 0
    for catalog in args.catalog:
        print(f"===== {catalog}")
        wh = Warehouse(session, host, args.warehouse_id, catalog)
        steps = plan(all_changesets, wh.applied(create=not args.dry_run))
        total += migrate(steps, wh.execute, wh.record, dry_run=args.dry_run)
        if total:
            break   # do not touch the next environment after a failure
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
