#!/usr/bin/env python3
"""Recover a legacy hostname change only on the confirmed original machine."""
import argparse
import json
from pathlib import Path

from evm_state.ch import ClickHouse
from evm_state.host_recovery import rebind


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--previous-host", required=True)
    parser.add_argument("--confirm-original-machine", action="store_true", required=True,
                        help="attest this is the original machine, not a copied checkout or remote writer")
    args = parser.parse_args()
    print(json.dumps(rebind(ClickHouse(args.database), args.state_dir, args.previous_host), indent=2))
