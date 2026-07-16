from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .api import serve
from .database import Database
from .engine import MemoryEngine


def main() -> None:
    p = argparse.ArgumentParser(prog="tme")
    p.add_argument("--db", default=os.getenv("TME_DB", "data/tme.db"))
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("serve"); s.add_argument("--host", default=os.getenv("TME_HOST", "127.0.0.1")); s.add_argument("--port", type=int, default=int(os.getenv("TME_PORT", "8080")))
    w = sub.add_parser("wallet-add"); w.add_argument("address"); w.add_argument("--label", default="")
    c = sub.add_parser("wallet-command"); c.add_argument("address"); c.add_argument("action")
    f = sub.add_parser("ingest-fill"); f.add_argument("json")
    sub.add_parser("report")
    args = p.parse_args(); engine = MemoryEngine(Database(args.db))
    if args.command == "serve": serve(engine, args.host, args.port, Path(__file__).parent.parent / "web")
    elif args.command == "wallet-add": print(json.dumps(engine.add_wallet(args.address, args.label), indent=2))
    elif args.command == "wallet-command": print(json.dumps(engine.command_wallet(args.address, args.action), indent=2))
    elif args.command == "ingest-fill": print(json.dumps(engine.ingest_fill(json.loads(args.json)), indent=2))
    elif args.command == "report": print(json.dumps(engine.report(), indent=2))


if __name__ == "__main__": main()

