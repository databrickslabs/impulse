# Fresh-eyes deployability audit — NuScenes demo (2026-05-22)

Read-only audit of `repos/impulse/` from the perspective of a stranger SA. Scope: end-to-end deploy of the NuScenes mini demo.

## Phase 1 — Prereqs

**Verdict:** unclear — CLI version floor is given but `uv` version is not; `databricks auth` step is implied rather than spelled out.

- `bundles/lakevision_demo/README.md:14-17` names Databricks CLI ≥ v1.0.0, `uv` (no version), and an authenticated profile in `~/.databrickscfg`. UC grants are stated as `USE CATALOG` + `CREATE SCHEMA` — clear.
- `CONTRIBUTING.md:18-32` covers `uv`/Python/Java for local development, but a deployer who only wants to push to a workspace doesn't need that whole stack and the bundle README doesn't tell them so.
- No pointer to `databricks auth login` or how to create a profile if one doesn't exist. A new SA will hit a confusing CLI error before they get to the bundle.

**Fix:** in `bundles/lakevision_demo/README.md` Prerequisites, add a `uv` version floor (matches what `uv build --wheel` expects) and one line: `databricks auth login --host <ws-url> --profile <name>` with a link to the CLI auth docs.

## Phase 2 — Dataset acquisition

**Verdict:** clear.

- `demos/byod/adapters/nuscenes/README.md:11-29` names the EULA URL (https://www.nuscenes.org/sign-up), the exact tarball URL (https://www.nuscenes.org/data/v1.0-mini.tgz, ~4.2 GB), and gives a ready-to-paste server-side download snippet.
- The default variant (`v1.0-mini`) is set in `bundles/lakevision_demo/databricks.yml:60` and surfaced in the adapter README — variant is unambiguous.

## Phase 3 — Unzip / staging

**Verdict:** clear.

- `demos/byod/adapters/nuscenes/README.md:37-45` shows the exact post-extraction layout: `maps/`, `samples/`, `sweeps/`, `v1.0-mini/`. Verifiable without running anything.
- `demos/byod/adapters/nuscenes/download.py:17,20-28` codifies the same four required dirs via `EXPECTED_SUBDIRS`. The README and code agree.

## Phase 4 — Deploy

**Verdict:** unclear — required `--var "catalog=..."` is documented but the deploy command in two READMEs disagrees on whether to pass `--profile`.

- `bundles/lakevision_demo/README.md:22-31` shows `--target shared --profile <ws> --var "catalog=..."`. Required-vs-default vars are called out explicitly. Good.
- `demos/byod/adapters/nuscenes/README.md:51-56` shows the same command without `--profile`. New SAs reading the adapter README first will hit "default profile" surprises if they have multiple workspaces configured.
- `--target shared` (production mode) vs the `dev` default (`databricks.yml:72-83`) is not explained in the bundle README — a stranger might pick the default and then be confused about why their bundle landed in `~/.bundle/...` instead of `/Workspace/Shared/...`.

**Fix:** in `bundles/lakevision_demo/README.md`, add a one-liner under "First-time deploy" explaining that `--target shared` writes to a shared workspace path so multiple SAs see the same bundle (the `dev` default is per-user). Reconcile the adapter README example to include `--profile` for consistency.

## Phase 5 — Run

**Verdict:** clear.

- `bundles/lakevision_demo/README.md:66-77` introduces `bootstrap_job` before `run_all_job` with a one-line explanation of the difference (bootstrap = tables + metadata only; run_all = full 4-notebook pipeline). 
- `bundles/lakevision_demo/resources/jobs.yml:76` sets the runtime expectation: "~25 min on NuScenes mini".

## Phase 6 — Verify

**Verdict:** unclear — the SQL block uses a literal `<adapter>_demo_` placeholder that a stranger may copy-paste verbatim.

- `bundles/lakevision_demo/README.md:96-100` provides three SQL spot-checks. The cross-table invariant and `LIST` are runnable as-is. 
- `bundles/lakevision_demo/README.md:97` reads `lakevision_demo_gold.<adapter>_demo_event_instance_fact`. A stranger sees the inline note about `nuscenes_demo_event_instance_fact` two lines up but the SQL itself isn't a paste-ready query.
- Expected counts: "NuScenes mini yields 9 windows" is a concrete check. The `n_events` value has no expected number — `> 0` is fine but a ballpark would shorten the "is this right?" loop.

**Fix:** in `bundles/lakevision_demo/README.md`, change the SQL to a NuScenes-defaults paste-ready form (`nuscenes_demo_event_instance_fact`) and add an expected `n_events` ballpark for v1.0-mini. Note in a comment that the prefix derives from the `adapter` var.

## Phase 7 — Interactive use

**Verdict:** unclear — the absolute path to `notebook_env.yml` is correct only after a `--target shared` deploy.

- `bundles/lakevision_demo/README.md:114-118` gives the right click-path (Environment side panel → Custom → file picker) and a fully qualified absolute path: `/Workspace/Shared/.bundle/lakevision_demo/files/bundles/lakevision_demo/notebook_env.yml`.
- That path is only valid for `--target shared` (production mode, `databricks.yml:79-83`). If the stranger deployed with the default `dev` target, the file lives under `${workspace.current_user.userName}` and the README's absolute path is wrong with no hint why.
- `notebook_env.yml:28-29` pins specific wheel filenames (`databricks_impulse-0.0.2-py3-none-any.whl`, `byod_demo-0.1.0-py3-none-any.whl`). Any version bump in `VERSION` or `demos/byod/pyproject.toml` silently breaks Custom-environment attachment — the wheel artifact still uploads, but with a different filename.

**Fix:** in `bundles/lakevision_demo/README.md` Interactive notebook use, note that the absolute path assumes `--target shared`. Add a one-line warning in `notebook_env.yml` (or `jobs.yml`) that the two wheel filenames must be edited if the project version bumps.

## Summary

- **Clear:** 3 (Phases 2, 3, 5).
- **Unclear:** 4 (Phases 1, 4, 6, 7).
- **Blocked:** 0.

Top 3 friction points:

1. **Phase 7 absolute path assumes `--target shared`** — silent failure mode if a stranger deploys to `dev` and copy-pastes the README path into the Environment side panel.
2. **Phase 1 auth bootstrap missing** — README says "an authenticated profile" but doesn't link to `databricks auth login`; a brand-new SA hits an opaque CLI error.
3. **Phase 6 SQL contains a `<adapter>_demo_` placeholder mid-query** — paste-and-run breaks until the stranger rereads the note 2 lines above.

Candidate follow-up BL entries — **all 5 resolved inline on 2026-05-22, did not file as BL entries.** See CHANGELOG `[Unreleased]` 2026-05-22 "audit-fix pass" entry.

- ~~Add `databricks auth login` bootstrap step and `uv` version floor to bundle README Prerequisites.~~ → applied to `bundles/lakevision_demo/README.md` Prerequisites: uv≥0.4 floor + a `databricks auth login` snippet with link to CLI docs.
- ~~Make Phase 6 verify-the-run SQL paste-ready.~~ → applied to `bundles/lakevision_demo/README.md` "Verify the run": SQL now uses `nuscenes_demo_*` defaults with `expect for NuScenes v1.0-mini: n_events = 9, n_windows = 9, 9 OpenLABEL JSONs` ballparks.
- ~~Document `--target shared` vs `dev` path differences.~~ → applied to `bundles/lakevision_demo/README.md` in two places: a callout under "First-time deploy" explaining the `dev`-prefix collision, and a callout under "Interactive notebook use" warning that the absolute paths assume `shared`. Also swept stale `--target dev` examples in `demos/byod/adapters/{a2d2,pandaset,zod}/README.md`.
- ~~Wheel filename drift warning.~~ → applied to both `bundles/lakevision_demo/notebook_env.yml` AND `bundles/lakevision_demo/resources/jobs.yml` (the audit only flagged notebook_env.yml; jobs.yml had the same pinned-filename risk — both files now carry a `!!! WHEEL FILENAME DRIFT !!!` callout with the upstream sources to bump).
- ~~Reconcile adapter README deploy command.~~ → applied to `demos/byod/adapters/nuscenes/README.md` (add `--profile`); same sweep covered a2d2, pandaset, zod (all also missing `--profile`).

**Stranger-deployable today: yes-with-fixes-applied** — the docs friction the audit surfaced is closed. **Real-workspace half (BL-024) is still required** to confirm the deploy succeeds end-to-end on a fresh FEVM workspace; that pass may surface new friction the docs-only audit could not detect.
