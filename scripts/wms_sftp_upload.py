"""
wms_sftp_upload.py – Play the warehouse system: drop the nightly stock CSV files on the WMS SFTP drop.

A real WMS uploads its export to the SFTP drop's inbox/ (infra/wms_sftp.tf); Data Factory then copies
it into the data lake (infra/adf.tf). scripts/up.ps1 runs this with the values of the terraform output
wms_sftp, after reading the password from Key Vault into an environment variable:

    $env:WMS_SFTP_PASSWORD = az keyvault secret show --vault-name <kv> --name wms-sftp-password --query value -o tsv
    python scripts/wms_sftp_upload.py --host <account>.blob.core.windows.net --username <account>.wms
    Remove-Item Env:WMS_SFTP_PASSWORD

The password comes from an environment variable, never an argument (arguments show up in process
lists and shell history), and is never printed. The wms user may create and write files but not read
or delete them, so a night that is uploaded again simply overwrites its file.

Host key: Azure Storage publishes its SFTP host keys per region. This demo accepts the key with a
warning; production pins the published fingerprint.
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Callable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "inventory"

log = logging.getLogger("wms_sftp_upload")


def files_to_upload(source_dir: Path, pattern: str) -> List[Path]:
    files = sorted(p for p in Path(source_dir).glob(pattern) if p.is_file())
    if not files:
        raise SystemExit(f"No {pattern} in {source_dir}. Run python -m data_generators.generate_inventory first.")
    return files


def ensure_folder(sftp, folder: str) -> None:
    try:
        sftp.stat(folder)
    except FileNotFoundError:
        sftp.mkdir(folder)


def connect(host: str, username: str, password: str, port: int = 22):
    import paramiko                                    # only needed for a real upload, not for the tests

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())
    client.connect(host, port=port, username=username, password=password,
                   look_for_keys=False, allow_agent=False, timeout=30)
    return client


def upload(host: str, username: str, password: str, files: List[Path], folder: str = "inbox",
           connect_fn: Callable = connect) -> List[str]:
    """Put each file into folder/ on the drop; returns the remote paths."""
    client = connect_fn(host, username, password)
    try:
        sftp = client.open_sftp()
        ensure_folder(sftp, folder)
        remote = []
        for path in files:
            target = f"{folder}/{path.name}"
            sftp.put(str(path), target)
            log.info("uploaded %s (%d bytes) -> %s", path.name, path.stat().st_size, target)
            remote.append(target)
        return remote
    finally:
        client.close()


def main(argv: Optional[list] = None, connect_fn: Callable = connect) -> List[str]:
    parser = argparse.ArgumentParser(description="Upload the WMS nightly stock CSV files to the SFTP drop.")
    parser.add_argument("--host", required=True, help="<account>.blob.core.windows.net (terraform output wms_sftp)")
    parser.add_argument("--username", required=True, help="<account>.wms (terraform output wms_sftp)")
    parser.add_argument("--folder", default="inbox")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--pattern", default="inventory_*.csv")
    parser.add_argument("--password-env", default="WMS_SFTP_PASSWORD", help="Environment variable holding the password")
    args = parser.parse_args(argv)

    password = os.environ.get(args.password_env)
    if not password:
        raise SystemExit(f"Set {args.password_env} to the wms user's password (Key Vault secret wms-sftp-password).")
    files = files_to_upload(args.source_dir, args.pattern)
    remote = upload(args.host, args.username, password, files, args.folder, connect_fn)
    print(f"{len(remote)} file(s) in {args.folder}/ on {args.host}")
    return remote


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    main()
