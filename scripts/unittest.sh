#!/bin/bash
# Runs the server test suite (fast set). Extra args go to pytest, e.g. -m slow
cd "$(dirname "$0")/.." && exec .venv/bin/python -m pytest "$@"
