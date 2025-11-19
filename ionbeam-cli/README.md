# IonBeam CLI

Command line interface for managing ionbeam services and data sources.

## Installation

This package is part of the ionbeam monorepo workspace. Install the entire workspace with:

```bash
uv sync
```

## Usage

The CLI automatically discovers and registers all ionbeam plugin commands:

```bash
ionbeam --help
```

Each data source and service provides its own subcommand through the `ionbeam.cli_plugins` entry point.