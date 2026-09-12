#!/usr/bin/env python3
"""Verify legacy PostgreSQL state at one coherent finalized database head."""
from evm_state.postgres import main

if __name__ == "__main__":
    main(complete=False)
