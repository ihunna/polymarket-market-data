#!/usr/bin/env python3
"""Pull the bot's CSV logs from the server into this folder.

Usage:
    python pull_data.py                     # default files, prompts for the SSH password
    python pull_data.py solana-15-updown.csv
    PM_SSH_PASSWORD=... python pull_data.py # non-interactive

The password is only held in memory / the child process environment; it is never
written to disk or passed on the command line.
"""
import argparse
import getpass
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_HOST = "root@187.7.17.28"
DEFAULT_REMOTE_DIR = "~/polymarket-market-data"
DEFAULT_FILES = [
    "solana-15-flat-dual-trades.csv",
    "solana-15-delta-side-trades.csv",
    "solana-15-updown.csv",
]
PASSWORD_ENV = "PM_SSH_PASSWORD"


def make_askpass(tmp: Path) -> Path:
    script = tmp / "askpass.sh"
    script.write_text(f'#!/bin/sh\nprintf "%s\\n" "${PASSWORD_ENV}"\n')
    script.chmod(stat.S_IRWXU)
    return script


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull CSV logs from the bot server.")
    parser.add_argument("files", nargs="*", default=DEFAULT_FILES, help="remote file names to pull")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    parser.add_argument("--dest", default=str(Path(__file__).resolve().parent))
    args = parser.parse_args()

    password = os.environ.get(PASSWORD_ENV) or getpass.getpass(f"SSH password for {args.host}: ")
    dest = Path(args.dest)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        env = {
            **os.environ,
            PASSWORD_ENV: password,
            "SSH_ASKPASS": str(make_askpass(tmp)),
            "SSH_ASKPASS_REQUIRE": "force",
            "DISPLAY": os.environ.get("DISPLAY", ":0"),
        }
        sources = [f"{args.host}:{args.remote_dir.rstrip('/')}/{name}" for name in args.files]
        cmd = [
            "scp", "-q",
            "-o", "PreferredAuthentications=password,keyboard-interactive",
            "-o", "NumberOfPasswordPrompts=1",
            "-o", "StrictHostKeyChecking=accept-new",
            *sources, str(tmp),
        ]
        result = subprocess.run(cmd, env=env, stdin=subprocess.DEVNULL)

        pulled, missing = [], []
        for name in args.files:
            src = tmp / name
            if src.exists():
                shutil.move(str(src), dest / name)
                pulled.append(name)
            else:
                missing.append(name)

    for name in pulled:
        size = (dest / name).stat().st_size
        print(f"✅ {name} ({size:,} bytes)")
    for name in missing:
        print(f"❌ {name} not pulled")
    if result.returncode != 0 and not pulled:
        print("scp failed (wrong password or host unreachable?)", file=sys.stderr)
    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
