"""
Code related to keeping track of the history of messages as they pass through the system.
"""

import subprocess
import dataclasses
from typing import Literal, Any
from pathlib import Path


@dataclasses.dataclass
class CodeSourceInfo:
    repo_status: Literal["Not Found", "Clean", "Dirty"]
    git_hash: str | None


def describe_code_source() -> CodeSourceInfo:
    try:
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


@dataclasses.dataclass
class PreviousActionInfo:
    action: ActionInfo
    message: MessageInfo | None
