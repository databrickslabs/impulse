# Databricks notebook source
# MAGIC %md
# MAGIC ### Download the Hugging Face dataset into a Unity Catalog Volume
# MAGIC
# MAGIC Copies every file of `renumics/synthetic-vehicle-fleet-telemetry` (~52 GB, 1,038 files)
# MAGIC into `/Volumes/<catalog>/<schema>/<volume>/<dataset folder>/`, preserving the repo layout.
# MAGIC * Pinned to a dataset `revision` (commit sha) so the copy is reproducible
# MAGIC * Resumable: files already present with the expected size are skipped
# MAGIC * Files are streamed straight from Hugging Face to the volume (needs outbound internet)

# COMMAND ----------

import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

dbutils.widgets.text("catalog", "impulse_testing_catalog")
dbutils.widgets.text("schema", "impulse_renumics")
dbutils.widgets.text("volume", "raw_data")
dbutils.widgets.text("hf_repo", "renumics/synthetic-vehicle-fleet-telemetry")
dbutils.widgets.text("revision", "017ec8f02e0753d8b092cf9229e1372d92a7b635")
dbutils.widgets.text("workers", "16")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
repo = dbutils.widgets.get("hf_repo")
revision = dbutils.widgets.get("revision")
workers = int(dbutils.widgets.get("workers"))

dest = f"/Volumes/{catalog}/{schema}/{volume}/{repo.split('/')[-1]}"
print("destination:", dest)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# COMMAND ----------


def list_files():
    url = f"https://huggingface.co/api/datasets/{repo}/tree/{revision}?recursive=true"
    files = []
    while url:
        with urllib.request.urlopen(url) as r:
            files += [f for f in json.load(r) if f["type"] == "file"]
            link = r.headers.get("Link", "")
        url = link.split(";")[0].strip("<>") if 'rel="next"' in link else None
    return files


def download(f):
    target = os.path.join(dest, f["path"])
    if os.path.exists(target) and os.path.getsize(target) == f["size"]:
        return f["size"]
    os.makedirs(os.path.dirname(target), exist_ok=True)
    url = f"https://huggingface.co/datasets/{repo}/resolve/{revision}/{f['path']}"
    for attempt in range(5):
        try:
            tmp = target + ".part"
            with urllib.request.urlopen(url, timeout=120) as r, open(tmp, "wb") as out:
                while chunk := r.read(8 << 20):
                    out.write(chunk)
            if os.path.getsize(tmp) != f["size"]:
                raise IOError(f"size mismatch for {f['path']}")
            os.replace(tmp, target)
            return f["size"]
        except Exception as e:
            print(f"retry {attempt + 1} {f['path']}: {e}", flush=True)
            time.sleep(2**attempt)
    raise RuntimeError(f"failed: {f['path']}")


# COMMAND ----------

files = list_files()
total = sum(f["size"] for f in files)
print(f"{len(files)} files, {total / 1e9:.2f} GB")

done, failed = 0, []
with ThreadPoolExecutor(workers) as pool:
    futures = {pool.submit(download, f): f for f in files}
    for i, fut in enumerate(as_completed(futures), 1):
        try:
            done += fut.result()
        except Exception as e:
            failed.append(futures[fut]["path"])
            print(e, flush=True)
        if i % 50 == 0 or i == len(files):
            print(f"{i}/{len(files)} files, {done / 1e9:.2f}/{total / 1e9:.2f} GB", flush=True)

if failed:
    raise RuntimeError(f"{len(failed)} files failed (re-run to resume): {failed[:10]}")
print(f"done — source_path for ingest: {dest}/assets")
