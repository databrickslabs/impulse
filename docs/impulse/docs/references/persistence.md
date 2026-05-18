---
sidebar_position: 6
title: Persist Module
---

# Persist Module

## Overview

The `persist` module provides data persistence and storage capabilities for Impulse.
This module handles the storage and retrieval of dimensional data, events, and facts within the reporting system.

## Module Structure

The persist module is organized into the following components:

### Core Components

- **`dimension_schema.py`** - Defines schema structures for dimensional data used in analytical reporting (including `event_dimension`).
- **`fact_schema.py`** - Manages schema definitions for fact tables in the data warehouse (including `event_instance_fact`).
- **`report_storage.py`** - Handles creation and management for storage writers and sinks.

### Architecture

The module follows the factory pattern for writer creation to support new report entities in the future.

To allow new sinks to be created we defined a interface that all sinks must implement.
This ensures that any new sink can be integrated seamlessly into the existing framework.


The persist module serves as the foundation for data storage within Impulse, ensuring reliable
and structured persistence of analytical data.
