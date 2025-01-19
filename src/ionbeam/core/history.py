"""
Code related to keeping track of the history of messages as they pass through the system.
"""

import dataclasses
import subprocess
from pathlib import Path
from typing import Any, Literal

from ..core.html_formatters import message_to_html


@dataclasses.dataclass
class CodeSourceInfo:
    repo_status: Literal["Not Found", "Clean", "Dirty"]
    git_hash: str | None


def describe_code_source(repo_path=None) -> CodeSourceInfo:
    try:
        if repo_path is None:
            repo_path = Path(__file__).parents[1]
        status = (
            "Dirty"
            if subprocess.check_output(
                ["git", "status", "--porcelain"],
                stderr=subprocess.STDOUT,
                cwd=repo_path,
            ).strip()
            else "Clean"
        )
        hash = (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path,
            )
            .decode("ascii")
            .strip()
        )
        return CodeSourceInfo(status, hash)

    # TODO: Implement a way for docker containers to know what git hash was used to create them.
    except (subprocess.CalledProcessError, FileNotFoundError):
        return CodeSourceInfo("Not Found", None)


@dataclasses.dataclass
class ActionInfo:
    name: str
    code: CodeSourceInfo | None


@dataclasses.dataclass
class MessageInfo:
    name: str
    metadata: Any

    def __repr_html_(self):
        return message_to_html(self)


@dataclasses.dataclass
class PreviousActionInfo:
    action: ActionInfo
    message: MessageInfo | None
