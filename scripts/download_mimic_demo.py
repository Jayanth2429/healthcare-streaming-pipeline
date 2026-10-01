"""Download the public MIMIC-IV demo tables used by this project.

The MIMIC-IV demo is openly accessible under the Open Data Commons Open
Database License (ODbL) v1.0. This script downloads only the two small tables
needed for the streaming replay: admissions and transfers.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from urllib.request import urlretrieve

BASE = "https://physionet.org/files/mimic-iv-demo/2.2/hosp"
FILES = ("admissions.csv.gz", "transfers.csv.gz")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/raw/mimic")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for name in FILES:
        target = output_dir / name
        print(f"Downloading {name} -> {target}")
        urlretrieve(f"{BASE}/{name}", target)

    print("Done. See DATA_SOURCES.md for attribution and license details.")


if __name__ == "__main__":
    main()
