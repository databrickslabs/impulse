---
sidebar_position: 2
title: Requirements
---

# Requirements

## Workspace

- **Unity Catalog.**
- **[Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/) with user
  authorization**, so the app can act with the signed-in user's permissions.
- **A SQL warehouse** for the app's queries. Each user can choose another one under Settings.
- **Serverless jobs.** Every report runs as a serverless notebook job.
- **Foundation Model API chat endpoints** for the assistant. The app uses the first available
  Claude Sonnet or GPT endpoint; each user can choose another chat model under Settings.
- **Access from serverless compute to GitHub**, where report jobs download the pinned Impulse
  wheel. Without it, see [Serverless without GitHub access](install.md#serverless-without-github-access).

## Data

A silver schema in the Impulse layout: `container_metrics`, `channel_metrics` and `channels`,
optionally `container_tags` and `channel_tags`, as described in
[Silver Layer Schema](/docs/data_model/silver_layer_schema). The Impulse demo data works
unchanged. Other layouts work through the app's [data model](install.md#data-model).

## Permissions

Users need:

- `USE CATALOG` and `USE SCHEMA` on the silver and gold catalogs and schemas, and `SELECT` on the
  silver tables.
- `CREATE TABLE` and `MODIFY` on the gold schema.
- `CAN_USE` on the app's SQL warehouse, or on the one they choose under Settings, and `CAN_QUERY`
  on the chat model endpoint.

Report jobs run as the app's service principal. It needs `USE CATALOG`, `USE SCHEMA` and `SELECT`
on the silver data, and additionally `MODIFY` and `CREATE TABLE` on the gold schema; see
[Grant the service principal access](install.md#4-grant-the-service-principal-access).

## Tools for the installation

- Git.
- The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/), authenticated against
  the workspace. The installation was tested with v1.21.0.
- Node.js 22.12 or later, to build the frontend.

Python isn't needed for the installation: the Databricks Apps runtime installs the app's Python
dependencies.
