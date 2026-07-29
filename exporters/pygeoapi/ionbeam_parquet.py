# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""pygeoapi Parquet provider for the ionbeam canonical store.

Upstream's provider reaches S3 through s3fs, which misreads SeaweedFS's
versioned-bucket directory markers: HEAD on the collection prefix answers as a
0-byte file, so pyarrow tries to parse the prefix itself as parquet. Instead,
list the prefix with pyarrow's native S3 filesystem and build the dataset from
the real ``.parquet`` objects. Everything past construction is inherited.

Mirrors ``ParquetProvider.__init__`` of the pinned image (pygeoapi 0.23.4) —
re-check when bumping the base image.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import pyarrow.compute as pc
import pyarrow.dataset
import pyarrow.fs as pafs
from pygeoapi.provider.base import ProviderConnectionError, ProviderItemNotFoundError
from pygeoapi.provider.parquet import ParquetProvider
from pygeoapi.provider.base import BaseProvider

LOGGER = logging.getLogger(__name__)

# Build-file naming is owned by ionbeam.builds (build_file_key):
#   <dataset>/ib_year=YYYY/ib_month=MM/ib_day=DD/<start stamp>_<span>-v<N>-<hash>.parquet
# The hive segments are inert here — the dataset is assembled from explicit
# fragments, so they never surface as columns.
_BUILD_FILE = re.compile(
    r'^(?P<dataset>.+)/ib_year=\d{4}/ib_month=\d{2}/ib_day=\d{2}/'
    r'(?P<window>(?P<stamp>\d{8}T\d{6})_(?P<span>[^-/]+))'
    r'-v(?P<version>\d+)-[0-9a-f]+\.parquet$')

# The subset of ISO-8601 durations ionbeam writes into build keys
# (isodate.duration_isoformat of a timedelta).
_SPAN = re.compile(
    r'^P(?:(?P<days>\d+)D)?'
    r'(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$')


def _latest_builds(paths):
    """The files to serve: per window only the highest version's file; the
    server's sweep deletes superseded versions once a rebuild settles. A path
    outside the layout is kept as-is."""
    windows = {}
    keep = []
    for path in paths:
        match = _BUILD_FILE.match(path)
        if match is None:
            keep.append(path)
            continue
        window = (match['dataset'], match['window'])
        windows.setdefault(window, {})[int(match['version'])] = path
    for versions in windows.values():
        keep.append(versions[max(versions)])
    return sorted(keep)

# Canonical feature ids are <UTC second stamp>-<sha1 hash> (ionbeam-client
# geo.py); the stamp bounds the row's time to a one-second interval.
_ID_STAMP = re.compile(r'^(\d{8}T\d{6})-[0-9a-f]{16}$')

# The store's geometry column (ionbeam-client geo.py GEOMETRY_FIELD). The
# inherited provider selects, decodes, and null-fills a column literally named
# 'geometry'; _read_parquet projects this one under that name.
_GEOMETRY_COLUMN = 'ib_geometry'


def _list_build_files(fs, prefix):
    """Every build file under the prefix, found through the layout itself:
    the ``_manifests`` registry names every window ever built, and a window's
    files live under its start day's partition directory. Only single
    directories are listed — never a recursive walk over the whole prefix,
    which SeaweedFS can answer with silently truncated, stale results."""
    infos = list(fs.get_file_info(pafs.FileSelector(prefix, recursive=False)))
    manifests = fs.get_file_info(pafs.FileSelector(
        f'{prefix}/_manifests', recursive=False, allow_not_found=True))
    days = {
        info.path.rsplit('/', 1)[-1][:8]
        for info in manifests
        if info.type == pafs.FileType.File
    }
    for day in sorted(days):
        infos += fs.get_file_info(pafs.FileSelector(
            f'{prefix}/ib_year={day[:4]}/ib_month={day[4:6]}/ib_day={day[6:8]}',
            recursive=False, allow_not_found=True))
    return sorted({
        info.path for info in infos
        if info.type == pafs.FileType.File
        and info.path.endswith('.parquet')
        and info.size > 0
    })


def _window_bounds(path):
    """The interval a file covers, parsed from the window stamp and span in
    its name, or None when the path is outside the layout — an unparsed file
    stays unpruned (always scanned), so correctness never rides on the naming
    convention."""
    match = _BUILD_FILE.match(path)
    if match is None:
        return None
    span = _SPAN.match(match['span'])
    if span is None or not any(span.groupdict().values()):
        return None
    start = datetime.strptime(match['stamp'], '%Y%m%dT%H%M%S').replace(
        tzinfo=timezone.utc)
    return start, start + timedelta(
        **{unit: int(n) for unit, n in span.groupdict().items() if n})


class IonbeamParquetProvider(ParquetProvider):
    def __init__(self, provider_def):
        # Deliberately skips ParquetProvider.__init__ (its s3fs path is the
        # bug being avoided); replicates it against a pyarrow filesystem.
        BaseProvider.__init__(self, provider_def)

        self.source = self.data.get('source')
        if not self.source:
            msg = "Need explicit 'source' attr in data field of provider config"
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        if self.source.startswith('s3://'):
            prefix = self.source.split('://', 1)[1].rstrip('/')
            self.fs = pafs.S3FileSystem(
                endpoint_override=os.environ.get('AWS_ENDPOINT_URL_S3') or None,
                region=os.environ.get('AWS_DEFAULT_REGION') or None,
            )
            try:
                files = _latest_builds(_list_build_files(self.fs, prefix))
            except OSError as err:  # prefix absent: dataset not built yet
                raise ProviderConnectionError(
                    f'no canonical data at {self.source}: {err}')
            if not files:
                raise ProviderConnectionError(
                    f'no canonical windows at {self.source} yet')
            self.ds = self._partitioned_dataset(files)
        else:
            self.fs = None
            self.ds = pyarrow.dataset.dataset(self.source)

        LOGGER.debug('Grabbing field information')
        self.get_fields()

        self.has_geometry = None not in [self.x_field, self.y_field]
        if self.has_geometry:
            self.minx = self.maxx = self.x_field
            self.miny = self.maxy = self.y_field
            self.bb = [self.minx, self.miny, self.maxx, self.maxy]
            geo = json.loads(self.ds.schema.metadata[b'geo'])
            self.crs = (geo['columns'][geo['primary_column']].get('crs')
                        or 'OGC:CRS84')

    def _partitioned_dataset(self, files):
        """Build the dataset with each window file's time interval declared as
        its partition expression, so a datetime query prunes to the overlapping
        windows by name — pyarrow never opens the other windows' footers (turns
        ~one metadata read per file into one per matching file). Files whose key
        doesn't parse stay unpruned, so pruning is an optimisation, not a
        correctness dependency."""
        fmt = pyarrow.dataset.ParquetFileFormat()
        schema = pyarrow.dataset.dataset(files, filesystem=self.fs).schema
        timefield = self.time_field and pc.field(self.time_field)
        fragments = []
        for path in files:
            expr = None
            bounds = _window_bounds(path) if timefield is not None else None
            if bounds is not None:
                start, end = bounds
                expr = (timefield >= pc.scalar(start)) & (timefield < pc.scalar(end))
            fragments.append(
                fmt.make_fragment(path, filesystem=self.fs, partition_expression=expr))
        return pyarrow.dataset.FileSystemDataset(
            fragments, schema=schema, format=fmt, filesystem=self.fs)

    def get(self, identifier, **kwargs):
        """Item by id, pruned by the id's time stamp.

        A canonical id carries the row's time (see the ``_ID_STAMP`` pattern),
        which the window partition expressions turn into a one-window scan.
        Every stored id is stamped, so an id that does not parse cannot exist
        and is rejected without a scan — ids are client input. Without a
        configured time field there is nothing to prune by; the inherited
        full scan serves the lookup."""
        if self.time_field is None:
            return super().get(identifier, **kwargs)

        match = _ID_STAMP.match(identifier)
        if match is None:
            raise ProviderItemNotFoundError(f'ID {identifier} not found')
        try:
            start = datetime.strptime(match.group(1), '%Y%m%dT%H%M%S').replace(
                tzinfo=timezone.utc)
        except ValueError:
            raise ProviderItemNotFoundError(f'ID {identifier} not found')
        timefield = pc.field(self.time_field)
        second = (timefield >= pc.scalar(start)) & (
            timefield < pc.scalar(start + timedelta(seconds=1)))
        fragments = list(self.ds.get_fragments(filter=second))
        if not fragments:
            raise ProviderItemNotFoundError(f'ID {identifier} not found')

        pruned = pyarrow.dataset.FileSystemDataset(
            fragments, schema=self.ds.schema, format=self.ds.format,
            filesystem=self.fs)
        original, self.ds = self.ds, pruned
        try:
            return super().get(identifier, **kwargs)
        finally:
            self.ds = original

    def _read_parquet(self, return_scanner=False, **kwargs):
        # pyarrow's scanner prefetches ~16 batches × 4 fragments ahead — a ~1 GB
        # spike per request against these wide windows, which OOMs the server
        # under the admin's concurrent per-collection polling. Cut read-ahead to
        # one so a request's live memory is ~a batch, not the prefetch buffer.
        # Batch size is left at the pyarrow default (well above a page), so the
        # provider's read-limit+1-then-slice paging stays exact.
        kwargs.setdefault('batch_readahead', 1)
        kwargs.setdefault('fragment_readahead', 1)
        # Serve ib_geometry under the literal name the inherited provider
        # hard-codes: without a column called 'geometry' it null-fills one and
        # every feature loses its position. A requested 'geometry' (appended by
        # the provider to explicit property selections) reads ib_geometry too.
        columns = kwargs.get('columns') or self.ds.schema.names
        kwargs['columns'] = {
            'geometry' if name == _GEOMETRY_COLUMN else name:
                pc.field(_GEOMETRY_COLUMN if name == 'geometry' else name)
            for name in columns
        }
        return super()._read_parquet(return_scanner=return_scanner, **kwargs)
