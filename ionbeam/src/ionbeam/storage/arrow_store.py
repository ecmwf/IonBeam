# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""Arrow storage - imports from ionbeam_client.

This module re-exports the ArrowStore interface and LocalFileSystemStore
implementation from ionbeam_client for backwards compatibility.

The transfer interface belongs in ionbeam_client as it's part of the client
contract and implementation details (filesystem, S3, etc.) that will be
swapped out in the future.
"""

from ionbeam_client.transfer import ArrowStore, LocalFileSystemStore

__all__ = ["ArrowStore", "LocalFileSystemStore"]
