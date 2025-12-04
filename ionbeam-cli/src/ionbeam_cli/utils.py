# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import click
from pydantic import BaseModel


def parse_datetime(date_str: str) -> datetime:
    try:
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise click.ClickException(
            f"Invalid date format '{date_str}'. Use ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS"
        ) from e


def load_json_model(file_path: str, model_class: type[BaseModel]) -> BaseModel:
    try:
        path = Path(file_path)
        if not path.exists():
            raise click.ClickException(f"File not found: {file_path}")
        
        with open(path) as f:
            data = json.load(f)
        
        return model_class(**data)
    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON in file: {e}")
    except Exception as e:
        raise click.ClickException(f"Failed to load file: {e}")


def sanitize_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())