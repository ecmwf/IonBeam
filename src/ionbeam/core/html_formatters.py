from dataclasses import fields, is_dataclass
import pandas as pd
import string, random
import pyodc as odc
from pathlib import Path
from typing import BinaryIO


def random_id(n):
    return "".join(random.choices(string.hexdigits, k=n))


def dataframe_to_html(data: pd.DataFrame, max_colwidth=100, **kwargs):
    kwargs = dict(index=False, notebook=True, render_links=True, max_rows=20) | kwargs

    with pd.option_context("display.max_colwidth", max_colwidth):
        return data.to_html(**kwargs)


def dict_to_html(d: dict):
    df = pd.DataFrame.from_records([dict(name=key, value=value) for key, value in d.items()])
    return dataframe_to_html(df)


def dataclass_to_html(dc):
    df = pd.DataFrame.from_records(
        [
            dict(
                name=field.name,
                # type=field.type,
                value=getattr(dc, field.name),
            )
            for field in fields(dc)
            if getattr(dc, field.name, None)
        ],
    )
    return dataframe_to_html(df)


def column_metadata_to_html(columns):
    df = pd.DataFrame.from_records(
        [
            dict(
                Name=c.name,
                Datatype=c.dtype,
                Unit=c.unit or "",
                Description=c.desc or "",
            )
            for c in columns
        ]
    )
    return dataframe_to_html(df)


def summarise_metadata(m):
    return ", ".join(
        str(v) for k in ["source", "observation_variable", "time_slice", "encoded_format"] if (v := getattr(m, k))
    )


def previous_action_info_to_html(action_info):
    if action_info.message is not None:
        msg_name = action_info.message.name
        msg_details = summarise_metadata(action_info.message.metadata)
        return f"""
            <li>
                <details>
                <summary>{msg_name}({msg_details}) → {action_info.action.name}</summary>
                    Previous Message
                    {dataclass_to_html(action_info.message)}
                    Action
                    {dataclass_to_html(action_info.action)}
                
                </details>
            </li>
        """
    else:
        return f"""
            <li>
                <details>
                <summary>{action_info.action.name}</summary>
                    Source
                    {dataclass_to_html(action_info.action)}
                </details>
            </li>
        """


def human_readable_bytes(n):
    for x in ["Bytes", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            break
        n /= 1024.0
    return f"{n:.0f} {x}"


def make_section(title, contents, open=False):
    return f"""
            <details {'open' if open else ''}>
              <summary>{title}</summary>
              {contents}
            </details>"""


def odb_info(filepath: str | BinaryIO) -> dict:
    "Read an ODB file and generate some useful info about it"
    r = odc.Reader(filepath)
    assert len(r.frames) == 1
    frame = r.frames[0]
    codecs = frame._column_codecs

    # Could do an optimisation here where only load the varying columns into memory
    # Would require peering into pyodc/codec.py to get the value of each constant codec
    df = frame.dataframe()

    varying_columns = [c for c in df if df[c].nunique() > 1]

    return dict(
        as_dataframe=df[varying_columns],
        properties=r.frames[0].properties,
        codecs=codecs,
        summary=pd.DataFrame(
            zip(
                df.dtypes,
                df.iloc[0],
                [df[c].nunique() for c in df],
                [c.name for c in codecs],
            ),
            columns=["dtype", "First Entry", "Unique Entries", "ODB codec"],
            index=df.columns,
        ),
    )


def odb_to_html(filepath: str | Path | BinaryIO):
    if isinstance(filepath, Path):
        info = odb_info(str(filepath))
    else:
        info = odb_info(filepath)

    properties = pd.DataFrame(info["properties"], index=[0]).transpose().to_html(header=False, notebook=True)
    summary = info["summary"].to_html(notebook=True)
    full_file = info["as_dataframe"].to_html(notebook=True, max_rows=20)

    return f"""

    <details open>
    <summary>Properties</summary>
        {properties}
    </details>
    
    <details>
    <summary>Summary</summary>
        {summary}
    </details>

    <details>
    <summary>Full Contents</summary>
        {full_file}
    </details>
    """


def summarise_file(filepath: Path):
    if not filepath.exists():
        return None
    size = human_readable_bytes(filepath.stat().st_size)
    if filepath.suffix == ".odb":
        return make_section(f"ODB File Data ({size})", odb_to_html(filepath))
    else:
        with open(filepath, "rb") as f:
            data = f.read(500).decode(errors="replace")
        if len(data) == 500:
            data += "..."
            size = human_readable_bytes(filepath.stat().st_size)
        return make_section(f"File Data ({size})", data)


def message_to_html(message):
    # Link the CSS and HTML using random ids so that if multiple
    # elements are emitted on the same page they don't interfere with one another.
    container_id = f"container-{random_id(15)}"
    inner_container_id = f"inner-container-{random_id(15)}"

    sections = []
    if hasattr(message, "reason"):
        sections.append(make_section("Reason", message.reason, open=True))

    if hasattr(message, "metadata"):
        sections.append(make_section("Metadata", dataclass_to_html(message.metadata), open=True))

    if hasattr(message, "columns") and message.columns:
        sections.append(
            make_section(
                "Column Metadata",
                column_metadata_to_html(message.columns),
            )
        )

    if getattr(message, "history", []):
        sections.append(
            make_section(
                "History",
                "<ol>" + "\n".join(previous_action_info_to_html(h) for h in message.history) + "</ol>",
                open=False,
            )
        )

    if hasattr(message, "data"):
        rows, columns = message.data.shape
        size = human_readable_bytes(message.data.memory_usage().sum())
        title = f"Tabular Data ({rows} rows x {columns} columns) ({size})"
        sections.append(make_section(title, dataframe_to_html(message.data)))

    if hasattr(message, "metadata") and getattr(message.metadata, "mars_request", {}):
        sections.append(make_section("Mars Request", message.metadata.mars_request._repr_html_()))

    if hasattr(message, "metadata") and getattr(message.metadata, "filepath", None) is not None:
        file_section = summarise_file(message.metadata.filepath)
        if file_section:
            sections.append(file_section)

    newline = "\n"
    details = f"({summarise_metadata(message.metadata)})" if hasattr(message, "metadata") else ""
    return f"""
        <style>
        #{container_id} summary:hover {{
                background: var(--jp-rendermime-table-row-hover-background);
            }}

        #{container_id} summary {{
            display: list-item !important;
        }}
        #{container_id} td {{
                text-align: left !important;
            }}
        #{container_id} h4 {{
            text-align: center;
            background-color: black;
            color: white;
            padding: 5px;
            margin: 0px !important;
        }}

        #{container_id} {{
            display: inline-flex;
            flex-direction: column;
            border: solid black 1px;
            border-radius: 5px;
            min-width: 100%;
            margin-top: 1em;
            margin-bottom: 1em;
        }}

        #{inner_container_id} {{
            width: 100%;
            padding: 10px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}

        details {{
        margin-left: 1em;
        }}
        </style>

        <div id="{container_id}">
        <h4>{message.__class__.__name__}{details}</h4>
        <div id="{inner_container_id}">
            {newline.join(sections)}
            </div>
        </div>
        """


def action_to_html(action, extra_sections=[]):
    # Link the CSS and HTML using random ids so that if multiple
    # elements are emitted on the same page they don't interfere with one another.
    container_id = f"container-{random_id(15)}"
    inner_container_id = f"inner-container-{random_id(15)}"

    sections, attributes = [], []
    for field in fields(action):
        value = getattr(action, field.name, None)
        if is_dataclass(value):
            sections.append(make_section(field.name.capitalize(), dataclass_to_html(value)))
        else:
            attributes.append([field.name, value])

    sections.extend(extra_sections)

    df = pd.DataFrame(attributes)
    html = dataframe_to_html(df, header=False)

    sections.insert(0, make_section("Attributes", html, open=True))

    newline = "\n"
    details = f"({summarise_metadata(action.metadata)})" if hasattr(action, "metadata") else ""
    return f"""
        <style>
        #{container_id} summary:hover {{
                background: var(--jp-rendermime-table-row-hover-background);
            }}

        #{container_id} summary {{
            display: list-item !important;
        }}
        #{container_id} td {{
                text-align: left !important;
            }}
        #{container_id} h4 {{
            text-align: center;
            background-color: black;
            color: white;
            padding: 5px;
            margin: 0px !important;
        }}

        #{container_id} {{
            display: inline-flex;
            flex-direction: column;
            border: solid black 1px;
            border-radius: 5px;
            min-width: 100%;
            margin-top: 1em;
            margin-bottom: 1em;
        }}

        #{inner_container_id} {{
            width: 100%;
            padding: 10px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}

        details {{
        margin-left: 1em;
        }}
        </style>

        <div id="{container_id}">
        <h4>{action.__class__.__name__}{details}</h4>
        <div id="{inner_container_id}">
            {newline.join(str(s) for s in sections)}
            </div>
        </div>
        """
