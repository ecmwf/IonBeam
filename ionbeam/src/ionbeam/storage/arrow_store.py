"""Arrow storage - imports from ionbeam_client.

This module re-exports the ArrowStore interface and LocalFileSystemStore
implementation from ionbeam_client for backwards compatibility.

The transfer interface belongs in ionbeam_client as it's part of the client
contract and implementation details (filesystem, S3, etc.) that will be
swapped out in the future.
"""

from ionbeam_client.transfer import ArrowStore, LocalFileSystemStore

__all__ = ["ArrowStore", "LocalFileSystemStore"]
