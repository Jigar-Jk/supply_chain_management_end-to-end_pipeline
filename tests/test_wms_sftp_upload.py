"""
test_wms_sftp_upload.py – The "WMS" upload to the SFTP drop (scripts/wms_sftp_upload.py) and its use in up.ps1.

No network: a fake SSH client records what would be sent. up.ps1 is read as text, as in
test_cd_workflow.py, to check that inventory now travels WMS -> SFTP -> Data Factory -> landing.

Run locally with:
    python -m unittest tests.test_wms_sftp_upload -v
"""

import importlib.util
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("wms_sftp_upload", ROOT / "scripts" / "wms_sftp_upload.py")
wms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wms)

PASSWORD = "s3cr3t-Pa55w0rd"
HOST = "stscmplatformwmsjk1.blob.core.windows.net"
USER = "stscmplatformwmsjk1.wms"


class FakeSftp:
    def __init__(self, folders=()):
        self.folders, self.files, self.made = set(folders), {}, []

    def stat(self, path):
        if path not in self.folders:
            raise FileNotFoundError(path)

    def mkdir(self, path):
        self.folders.add(path)
        self.made.append(path)

    def put(self, local, remote):
        self.files[remote] = Path(local).read_bytes()


class FakeClient:
    def __init__(self, sftp):
        self.sftp, self.closed = sftp, False

    def open_sftp(self):
        return self.sftp

    def close(self):
        self.closed = True


class TestUpload(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name)
        for night in ("2026-11-01", "2026-11-02"):
            (self.src / f"inventory_{night}.csv").write_bytes(b"warehouse_id\nWH-1\n")
        (self.src / "notes.txt").write_text("not a feed file", encoding="utf-8")
        self.sftp, self.calls = FakeSftp(), []

    def tearDown(self):
        self.tmp.cleanup()

    def fake_connect(self, host, username, password):
        self.calls.append((host, username, password))
        self.client = FakeClient(self.sftp)
        return self.client

    def run_main(self, env=None, extra=()):
        argv = ["--host", HOST, "--username", USER, "--source-dir", str(self.src), *extra]
        with mock.patch.dict(os.environ, env if env is not None else {"WMS_SFTP_PASSWORD": PASSWORD}, clear=True):
            return wms.main(argv, connect_fn=self.fake_connect)

    def test_uploads_each_nightly_csv_into_inbox(self):
        remote = self.run_main()
        self.assertEqual(remote, ["inbox/inventory_2026-11-01.csv", "inbox/inventory_2026-11-02.csv"])
        self.assertEqual(set(self.sftp.files), set(remote))
        self.assertEqual(self.sftp.files["inbox/inventory_2026-11-01.csv"], b"warehouse_id\nWH-1\n")

    def test_logs_in_as_the_wms_user_with_the_password_from_the_environment(self):
        self.run_main()
        self.assertEqual(self.calls, [(HOST, USER, PASSWORD)])
        self.assertTrue(self.client.closed)

    def test_creates_inbox_when_missing_and_reuses_it_otherwise(self):
        self.run_main()
        self.assertEqual(self.sftp.made, ["inbox"])
        self.sftp.made.clear()
        self.run_main()
        self.assertEqual(self.sftp.made, [])

    def test_without_the_password_it_stops_with_a_hint(self):
        with self.assertRaises(SystemExit) as caught:
            self.run_main(env={})
        self.assertIn("WMS_SFTP_PASSWORD", str(caught.exception))
        self.assertEqual(self.calls, [], "must not even try to connect")

    def test_no_files_is_an_error_not_an_empty_success(self):
        with self.assertRaises(SystemExit) as caught:
            self.run_main(extra=["--pattern", "nothing_*.csv"])
        self.assertIn("generate_inventory", str(caught.exception))

    def test_the_password_is_never_logged_or_printed(self):
        with self.assertLogs("wms_sftp_upload", logging.INFO) as logs, mock.patch("builtins.print") as printed:
            self.run_main()
        text = " ".join(logs.output) + " ".join(str(c) for c in printed.call_args_list)
        self.assertNotIn(PASSWORD, text)

    def test_the_password_is_not_an_argument(self):
        source = (ROOT / "scripts" / "wms_sftp_upload.py").read_text(encoding="utf-8")
        self.assertNotIn('add_argument("--password"', source)


class TestUpScript(unittest.TestCase):
    """scripts/up.ps1: inventory goes WMS -> SFTP drop -> Data Factory -> landing, the rest straight to landing."""

    @classmethod
    def setUpClass(cls):
        text = (ROOT / "scripts" / "up.ps1").read_text(encoding="utf-8")
        cls.code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))

    def test_inventory_is_no_longer_uploaded_to_landing(self):
        self.assertRegex(self.code, r'\$feeds\s*=\s*\$feeds \| Where-Object \{ \$_ -ne "inventory" \}')

    def test_the_wms_file_goes_to_the_sftp_drop_with_the_password_from_key_vault(self):
        self.assertIn("wms_sftp_upload.py", self.code)
        self.assertRegex(self.code, r"az keyvault secret show .*--name \$\(\$wmsSftp.password_secret\)")
        self.assertIn("Remove-Item Env:WMS_SFTP_PASSWORD", self.code)
        self.assertNotRegex(self.code, r"Write-(Host|Output).*WMS_SFTP_PASSWORD", "the password must never be shown")

    def test_data_factory_copies_it_and_the_script_waits_for_the_result(self):
        self.assertIn("az datafactory pipeline create-run", self.code)
        self.assertIn("--name copy_wms_inventory", self.code)
        self.assertIn("az datafactory pipeline-run show", self.code)
        self.assertRegex(self.code, r'"Succeeded"')

    def test_the_copy_runs_before_the_bundle_deploy(self):
        self.assertLess(self.code.index("az datafactory pipeline create-run"), self.code.index("databricks bundle deploy"))

    def test_skip_data_skips_the_wms_upload_too(self):
        start = self.code.index("if (-not $SkipData)")
        block = self.code[start:self.code.index("Deploying the bundle")]
        self.assertIn("wms_sftp_upload.py", block)


if __name__ == "__main__":
    unittest.main()
