import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="agent-vcr", description="Record/replay AI agent runs.")
    sub = p.add_subparsers(dest="cmd", required=True)
    # Subcommands added in later tasks; registered here as they land.
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    print(f"agent-vcr: no handler for {args.cmd!r} yet", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())