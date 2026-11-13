"""Compose demo entry point: functional scenarios + capacity/cancel demo.

Runs both demo programs against the two API processes::

    python -m examples.run_all_demos --base-a http://api-a:8000 \\
                                     --base-b http://api-b:8000
"""
from __future__ import annotations

import argparse

from examples.run_capacity_demo import main as capacity_demo
from examples.run_demo import main as functional_demo


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-a", default="http://localhost:8001")
    ap.add_argument("--base-b", default="http://localhost:8002")
    args = ap.parse_args(argv)
    demo_args = ["--base-a", args.base_a, "--base-b", args.base_b]

    functional_demo(demo_args)
    capacity_demo(demo_args)
    print("\n全部演示完成。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
