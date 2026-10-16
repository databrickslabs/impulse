"""Entry point of the e2e job: runs the tests/e2e suite with pytest."""

import argparse
import os
import sys

import pytest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--source-schema", required=True)
    # Passed in because `__file__` is not defined in serverless Python tasks.
    parser.add_argument("--repo-root", required=True)
    args, _ = parser.parse_known_args()
    os.environ["IMPULSE_E2E_CATALOG"] = args.catalog
    os.environ["IMPULSE_E2E_SOURCE_SCHEMA"] = args.source_schema
    os.chdir(args.repo_root)

    return pytest.main(
        [
            "tests/e2e",
            "-m",
            "e2e",
            "-v",
            "-p",
            "no:cacheprovider",
            "--no-cov",
            # Drop the repo's default coverage/junit options.
            "-o",
            "addopts=",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())
