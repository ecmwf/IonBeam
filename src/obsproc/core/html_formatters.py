from dataclasses import fields
import pandas as pd
import string, random


def random_id(n):
    return "".join(random.choices(string.hexdigits, k=n))


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
    with pd.option_context("display.max_colwidth", 100):
        html = df.to_html(
            index=False,
            header=False,
            notebook=True,
            render_links=True,
        )

    return html


def data_to_html(data: pd.DataFrame):
    return data.to_html(
        index=False,
        # notebook=True,
        render_links=True,
        max_rows=20,
    )


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
    with pd.option_context("display.max_colwidth", 100):
        return df.to_html(
            header=False,
            index=False,
            notebook=True,
            render_links=True,
        )


def summarise_metadata(m):
    return ", ".join(
        str(v)
        for k in ["source", "observation_variable", "time_slice", "encoded_format"]
        if (v := getattr(m, k))
    )


def previous_action_info_to_html(action_info):
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


def message_to_html(message):
    # Link the CSS and HTML using random ids so that if multiple
    # elements are emitted on the same page they don't interfere with one another.
    container_id = f"container-{random_id(15)}"
    inner_container_id = f"inner-container-{random_id(15)}"

    sections = []
    if hasattr(message, "reason"):
        sections.append(make_section("Reason", message.reason, open=True))

    if hasattr(message, "metadata"):
        sections.append(
            make_section("Metadata", dataclass_to_html(message.metadata), open=True)
        )

    if hasattr(message, "columns"):
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
                "<ol>"
                + "\n".join(previous_action_info_to_html(h) for h in message.history)
                + "</ol>",
                open=False,
            )
        )

    if hasattr(message, "data"):
        rows, columns = message.data.shape
        size = human_readable_bytes(message.data.memory_usage().sum())
        title = f"Tabular Data ({rows} rows x {columns} columns) ({size})"
        sections.append(make_section(title, data_to_html(message.data)))

    if (
        hasattr(message, "metadata")
        and getattr(message.metadata, "filepath", None) is not None
    ):
        try:
            with open(message.metadata.filepath, "r") as f:
                data = f.read(500)
            if len(data) == 500:
                data += "..."
            size = human_readable_bytes(message.metadata.filepath.stat().st_size)
            sections.append(make_section(f"File Data ({size})", data))
        except:
            pass

    newline = "\n"
    details = (
        f"({summarise_metadata(message.metadata)})"
        if hasattr(message, "metadata")
        else ""
    )
    return f"""
        <style>
        #{container_id} summary:hover {{
                background: var(--jp-rendermime-table-row-hover-background);
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
        </style>

        <div id="{container_id}">
        <h4>{message.__class__.__name__}{details}</h4>
        <div id="{inner_container_id}">
            {newline.join(sections)}
            </div>
        </div>
        """
