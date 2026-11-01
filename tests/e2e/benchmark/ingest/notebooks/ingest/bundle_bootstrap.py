# Databricks notebook source
"""Append bundled ``modules/`` to ``sys.path`` so ``utils`` imports cleanly."""

import os
import sys

_nb_dir = os.path.dirname(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_modules_path = os.path.normpath(f"/Workspace{_nb_dir}/../../modules")
if _modules_path not in sys.path:
    sys.path.append(_modules_path)
