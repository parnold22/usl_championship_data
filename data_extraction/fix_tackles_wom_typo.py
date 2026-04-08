#!/usr/bin/env python3
"""
Replace the typo "tackles_wom" with "tackles_won" in every file under
dbt_usl_championship/seeds/scraped_data/player_match_stats/.

Usage:
  python data_extraction/fix_tackles_wom_typo.py           # apply changes
  python data_extraction/fix_tackles_wom_typo.py --dry-run # print only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

OLD = "tackles_wom"
NEW = "tackles_won"


def default_dir(repo_root: Path) -> Path:
    return (
        repo_root
        / "dbt_usl_championship"
        / "seeds"
        / "scraped_data"
        / "player_match_stats"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dir",
        type=Path,
        default=None,
        help="Override player_match_stats directory (default: under repo root)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would change without writing",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    target = args.dir.resolve() if args.dir else default_dir(repo_root)
    if not target.is_dir():
        print(f"Not a directory: {target}", file=sys.stderr)
        return 1

    changed = 0
    scanned = 0
    for path in sorted(target.iterdir()):
        if not path.is_file():
            continue
        scanned += 1
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            print(f"Skip (not utf-8): {path}", file=sys.stderr)
            continue
        if OLD not in text:
            continue
        new_text = text.replace(OLD, NEW)
        n = text.count(OLD)
        if args.dry_run:
            print(f"Would update {path.name} ({n} replacement(s))")
        else:
            path.write_text(new_text, encoding="utf-8")
            print(f"Updated {path.name} ({n} replacement(s))")
        changed += 1

    print(f"Done. Scanned {scanned} file(s), modified {changed} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
