"""Consistent SQLite online backup; no calls or provider requests."""

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("database", help="Path to calls.sqlite3")
parser.add_argument("output", help="Backup directory")
args = parser.parse_args()
source = Path(args.database).resolve()
if not source.is_file():
    parser.error("Database does not exist")
folder = Path(args.output)
folder.mkdir(parents=True, exist_ok=True)
destination = folder / ("calls-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".sqlite3")
with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as src, sqlite3.connect(destination) as dst:
    src.backup(dst)
destination.chmod(0o600)
print(f"Backup saved: {destination}")
