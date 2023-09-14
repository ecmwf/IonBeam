from dataclasses import fields
import pandas as pd


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


def make_section(title, contents, open=False):
    return f"""
            <details {'open' if open else ''}>
              <summary>{title}</summary>
              {contents}
            </details>"""


def message_to_html(message):
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

    if hasattr(message, "data"):
        rows, columns = message.data.shape
        title = f"Tabular Data ({rows} rows x {columns} columns)"
        sections.append(make_section(title, data_to_html(message.data)))

    if getattr(message.metadata, "filepath", None) is not None:
        try:
            with open(message.metadata.filepath, "r") as f:
                data = f.read(500)
            if len(data) == 500:
                data += "..."
            sections.append(make_section("File Data", data))
        except:
            pass

    newline = "\n"
    return f"""
        <style>
        .message-container td {{
                text-align: left !important;
            }}
        div.message-container h4 {{
            text-align: center;
            background-color: black;
            color: white;
            padding: 5px;
            margin: 0px !important;
        }}

        div.message-container {{
            display: inline-flex;
            flex-direction: column;
            border: solid black 1px;
            border-radius: 5px;
            min-width: 100%;
            margin-top: 1em;
            margin-bottom: 1em;
        }}

        div.message-contents-container {{
            width: 100%;
            padding: 10px;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}
        </style>

        <div class="message-container">
        <h4>{message.__class__.__name__}</h4>
        <div class="message-contents-container">
            {newline.join(sections)}
            </div>
        </div>
        """
