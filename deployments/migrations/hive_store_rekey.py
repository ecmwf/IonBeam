# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
"""One-off canonical-store re-key: move every build file from the flat day
directory ``<dataset>/<YYYYMMDD>/`` to the hive day partition
``<dataset>/ib_year=YYYY/ib_month=MM/ib_day=DD/`` (ionbeam.builds layout).

Each file is server-side copied, size-verified, then deleted from its old
key, so an interrupted run resumes where it stopped and a re-run is a no-op.
Keys outside the old layout (``_manifests/``, pre-day-dir flat files) are
untouched. Run it before rolling the images that speak the new layout —
old-layout keys are invisible to them.

Usage:
    uv run hive_store_rekey.py s3://<bucket>/<prefix>     # e.g. s3://ionbeam/datasets
    uv run hive_store_rekey.py /path/to/local/store

S3 credentials and endpoint come from the standard AWS environment
(``AWS_ENDPOINT_URL_S3`` for SeaweedFS), exactly as the service reads them.
"""

import os
import re
import sys

import pyarrow.fs as pafs

_DAY_DIR = re.compile(r"^\d{8}$")


def _filesystem(target: str) -> tuple[pafs.FileSystem, str]:
    if target.startswith("s3://"):
        return pafs.S3FileSystem(
            endpoint_override=os.environ.get("AWS_ENDPOINT_URL_S3") or None,
            region=os.environ.get("AWS_DEFAULT_REGION") or None,
        ), target.removeprefix("s3://").rstrip("/")
    return pafs.LocalFileSystem(), target.rstrip("/")


def _subdirs(fs: pafs.FileSystem, path: str) -> list[pafs.FileInfo]:
    infos = fs.get_file_info(pafs.FileSelector(path, recursive=False, allow_not_found=True))
    return [info for info in infos if info.type == pafs.FileType.Directory]


def _rekey_day(fs: pafs.FileSystem, day_dir: pafs.FileInfo) -> tuple[int, int]:
    day = day_dir.base_name
    parent = day_dir.path.rsplit("/", 1)[0]
    new_dir = f"{parent}/ib_year={day[:4]}/ib_month={day[4:6]}/ib_day={day[6:8]}"
    moved = skipped = 0
    for info in fs.get_file_info(pafs.FileSelector(day_dir.path, recursive=False)):
        if info.type != pafs.FileType.File:
            continue
        new_path = f"{new_dir}/{info.base_name}"
        existing = fs.get_file_info(new_path)
        if existing.type == pafs.FileType.File and existing.size == info.size:
            fs.delete_file(info.path)  # copied by an interrupted run
            skipped += 1
            continue
        fs.copy_file(info.path, new_path)
        copied = fs.get_file_info(new_path)
        if copied.type != pafs.FileType.File or copied.size != info.size:
            raise RuntimeError(f"copy of {info.path} to {new_path} did not verify")
        fs.delete_file(info.path)
        moved += 1
    remaining = fs.get_file_info(pafs.FileSelector(day_dir.path, recursive=True, allow_not_found=True))
    if not any(info.type == pafs.FileType.File for info in remaining):
        fs.delete_dir(day_dir.path)
    return moved, skipped


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    fs, base = _filesystem(sys.argv[1])
    total_moved = total_skipped = 0
    for dataset_dir in _subdirs(fs, base):
        moved = skipped = 0
        for day_dir in _subdirs(fs, dataset_dir.path):
            if not _DAY_DIR.match(day_dir.base_name):
                continue
            day_moved, day_skipped = _rekey_day(fs, day_dir)
            moved += day_moved
            skipped += day_skipped
        print(f"{dataset_dir.base_name}: moved {moved}, already-done {skipped}")
        total_moved += moved
        total_skipped += skipped
    print(f"done: moved {total_moved}, already-done {total_skipped}")


if __name__ == "__main__":
    main()
