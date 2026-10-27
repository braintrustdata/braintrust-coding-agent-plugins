#!/usr/bin/env bash
# Validate the built marketplace, including each package's separate contract.
set -euo pipefail
python3 "$(dirname "${BASH_SOURCE[0]}")/package.py" validate "${1:?usage: validate.sh <TARGET_DIR>}"
