-include .env
export

APP ?= impulse-adhoc-reporting
BUNDLE_VAR_warehouse_id = $(DATABRICKS_WAREHOUSE_ID)
WHEEL := $(shell sed -n 's/^wheel = "\(.*\)"/\1/p' impulse.lock)
# Framework the contract tests build against: the pinned release wheel, or the source in this repo with FRAMEWORK=../..
FRAMEWORK ?= $(WHEEL)
# Install from uv.lock as it is, so a local package mirror can't rewrite it; only `make lock` changes it.
UV_RUN := uv run --frozen --python 3.11
DEPLOY_ENV := DATABRICKS_CONFIG_PROFILE DATABRICKS_WAREHOUSE_ID
WORKSPACE_ENV := $(DEPLOY_ENV) IMPULSE_DEMO_CATALOG IMPULSE_DEMO_SCHEMA
require = @for v in $(1); do [ -n "$$(printenv $$v)" ] || { echo "$$v is not set: copy .env.example to .env and fill it in"; exit 1; }; done

.PHONY: lock test live smoke smoke-persist eval seed frontend deploy logs

lock:
	uv lock --python 3.11
	@# Keep the lockfiles on the public registries, also when they were resolved through a Databricks mirror.
	@perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' uv.lock
	@perl -pi -e 's|https://pypi-proxy\.[a-z]+\.databricks\.com/|https://files.pythonhosted.org/|g' uv.lock
	@perl -pi -e 's|https://npm-proxy\.[a-z]+\.databricks\.com/|https://registry.npmjs.org/|g' frontend/package-lock.json
	uv export --format requirements-txt --no-hashes --no-dev --no-emit-project --no-annotate --no-header --frozen -o requirements.txt

test:
	$(UV_RUN) pytest -q tests/unit
	uv run --no-project --python 3.12 --with "databricks-impulse[local-dev] @ $(FRAMEWORK)" --with fastapi --with pyyaml --with pytest \
		pytest -q -p no:cacheprovider -W ignore::UserWarning tests/contract

live:
	$(call require,$(WORKSPACE_ENV))
	$(UV_RUN) pytest -q tests/live

smoke:
	$(call require,$(WORKSPACE_ENV))
	PYTHONPATH=. $(UV_RUN) python scripts/smoke_run.py --name demo_full --spec tests/fixtures/specs/demo_full.json

smoke-persist:
	$(call require,$(WORKSPACE_ENV))
	PYTHONPATH=. $(UV_RUN) python scripts/smoke_run.py --name demo_full_persist --spec tests/fixtures/specs/demo_full.json --persist

eval:
	$(call require,$(WORKSPACE_ENV))
	PYTHONPATH=. $(UV_RUN) python scripts/eval_agent.py

seed:
	$(call require,$(WORKSPACE_ENV))
	uv run --no-project --python 3.12 --with databricks-sdk python scripts/seed_demo.py

frontend:
	cd frontend && npm ci --silent && npm run build

deploy: frontend
	$(call require,$(DEPLOY_ENV))
	@git diff --quiet -- . && git diff --cached --quiet -- . || (echo "Commit changes before deploying" && exit 1)
	DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy
	DATABRICKS_BUNDLE_ENGINE=direct databricks bundle run impulse_app

logs:
	$(call require,DATABRICKS_CONFIG_PROFILE)
	databricks apps logs $(APP) --tail-lines 300
