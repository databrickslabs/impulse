"""Entry point for the serverless e2e job (see ``resources/e2e_job.yml``).

Databricks Asset Bundles sync the whole repo into the workspace, so this file
and the ``tests/e2e`` suite it runs sit next to each other at a stable path.
The job runs it as a ``spark_python_task``; a non-zero exit fails the task,
which fails ``databricks bundle run`` and therefore the CI job. Pytest output
(including assertion detail) is streamed into the job/run logs.

Args (positional, passed by the job):
    --catalog <name>   UC catalog for the ephemeral test schemas. Exported as
                       ``IMPULSE_E2E_CATALOG`` for the conftest fixtures.
"""

import argparse
import os
import sys

import pytest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=None)
    args, _ = parser.parse_known_args()
    if args.catalog:
        os.environ["IMPULSE_E2E_CATALOG"] = args.catalog

    # Repo root = two levels up from tests/e2e/run_in_job.py. Run pytest from
    # there so the tests/ package layout resolves as it does locally.
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(here, "..", ".."))
    os.chdir(repo_root)

    pytest_args = [
        "tests/e2e",
        "-m",
        "e2e",
        "-v",
        "-p",
        "no:cacheprovider",
        # This suite is not about coverage; the local suite owns that.
        "--no-cov",
        # Drop the repo's default coverage/junit addopts (they assume the local
        # env and a local junit path).
        "-o",
        "addopts=",
    ]
    return pytest.main(pytest_args)


if __name__ == "__main__":
    sys.exit(main())
