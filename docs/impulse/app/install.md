---
sidebar_position: 3
title: Install
---

# Install

Check the [requirements](requirements.md) first. The commands run on your machine with the
Databricks CLI authenticated against the workspace; `-p <profile>` selects the CLI profile.

## 1. Get the code

```bash
git clone https://github.com/databrickslabs/impulse.git
cd impulse/apps/adhoc-reporting
```

Use `main` or check out a release tag (`git checkout vX.Y.Z`). Either way, `impulse.lock` pins the
Impulse release that report jobs install.

## 2. Build the frontend

```bash
cd frontend && npm ci && npm run build && cd ..
```

## 3. Deploy

```bash
databricks bundle deploy --var warehouse_id=<warehouse id> -p <profile>
databricks bundle run impulse_app --var warehouse_id=<warehouse id> -p <profile>
```

`bundle run` starts the app and prints its URL. Pass the same `--var` values to both commands. The
bundle has these variables:

| Variable | Default | Meaning |
|---|---|---|
| `warehouse_id` | required | SQL warehouse the app runs user queries on |
| `app_name` | `impulse-app` | Name of the app, part of its URL |
| `end_user_group` | `users` | Group that gets `CAN_USE` on the app |

## 4. Grant the service principal access

Report jobs run as the app's service principal. A catalog or schema owner grants it access once
per silver and gold schema:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<service principal application id>`;
GRANT USE SCHEMA, SELECT ON SCHEMA <catalog>.<silver schema> TO `<service principal application id>`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA <catalog>.<gold schema> TO `<service principal application id>`;
```

The application id is the `service_principal_client_id` in the output of
`databricks apps get impulse-app -p <profile>`. Before each run, the Review & run step
checks the access of both the user and the service principal and shows the exact statements when
something is missing.

## Serverless without GitHub access

Report jobs install the Impulse wheel pinned in `impulse.lock` from its GitHub release. If
serverless compute can't reach GitHub, upload that wheel to a Unity Catalog volume, grant the app's
service principal `READ VOLUME` on the volume (and `USE CATALOG` and `USE SCHEMA` on its catalog
and schema), and add its path to `impulse.lock` before deploying:

```toml
wheel_override = "/Volumes/<catalog>/<schema>/<volume>/databricks_impulse-<version>-py3-none-any.whl"
```

The wheel's dependencies still come from PyPI or the workspace's package repository. Upload the
new wheel and update the path with each upgrade.

## Data model

Schemas in the Impulse layout need no configuration. Otherwise an admin edits the shared data model
on the app's **Data model** page; it's stored in `/Workspace/Shared/Impulse/config.yaml`. List only
the settings that differ, for example:

```yaml
silver_catalog: main
silver_schema: measurements
tables:
  channels: channel_data
uut:
  key: uut_id
solver_config:
  container_metrics:
    column_name_mapping:
      measurement_id: container_id
```

`solver_config` is passed to the Impulse query engine unchanged (see
[Configuration](/docs/config/configuration)). The page lists every setting with its default and
meaning, and tests the configuration against a schema with the admin's permissions before saving.

## Upgrade

Pull the new version or check out a newer release tag, build the frontend and deploy again as
above. Reports are stored in each user's workspace folder and keep working; each report job uses
the Impulse version pinned by the deployed app.
