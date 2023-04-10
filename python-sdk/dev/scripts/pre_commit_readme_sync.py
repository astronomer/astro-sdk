#!/usr/bin/env python
from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

TABLE_ALIGNMENT = Literal["<", ">", "^"]

SOURCES_ROOT = Path(__file__).parents[2]
PROJECT_ROOT = Path(__file__).parents[3]
ASTRO_ROOT = SOURCES_ROOT / "src" / "astro"
README_PATH = PROJECT_ROOT / "README.md"

HEADING = "## Supported technologies"


def get_constants_module():
    """
    Get constants module
    """
    spec = importlib.util.spec_from_file_location("constants", f"{ASTRO_ROOT}/constants.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Translation dictionaries for table alignment
left_rule = {"<": ":", "^": ":", ">": "-"}
right_rule = {"<": "-", "^": ":", ">": ":"}


def evaluate_field(record: list[Any], field_spec: Callable | str | int) -> str:
    """
    Evaluate a field of a record using the type of the field_spec as a guide.
    """
    if isinstance(field_spec, int):
        return str(record[field_spec])
    elif isinstance(field_spec, str):
        return str(getattr(record, field_spec))
    else:
        return str(field_spec(record))


def get_table(
    records: list[list[str]],
    fields: list,
    headings: list[str],
    alignment: list[tuple[TABLE_ALIGNMENT, TABLE_ALIGNMENT]] | None = None,
):
    """
    Generate a Doxygen-flavor Markdown table from records.

    :param records: Iterable. Rows will be generated from this.
    :param fields: List of fields for each row.  Each entry may be an integer,
        string or a function.  If the entry is an integer, it is assumed to be
        an index of each record.  If the entry is a string, it is assumed to be
        a field of each record.  If the entry is a function, it is called with
        the record and its return value is taken as the value of the field.
    :param headings: List of column headings.
    :param alignment: List of pairs alignment characters.  The first of the pair
        specifies the alignment of the header, (Doxygen won't respect this, but
        it might look good, the second specifies the alignment of the cells in
        the column.

        Possible alignment characters are:
            '<' = Left align (default for cells)
            '>' = Right align
            '^' = Center (default for column headings)
    """
    num_columns = len(fields)
    assert len(headings) == num_columns

    # Compute the table cell data
    columns: list[list[str]] = [[] for i in range(num_columns)]
    for record in records:
        for i, field in enumerate(fields):
            columns[i].append(evaluate_field(record, field))

    # Fill out any missing alignment characters.
    extended_align: list[tuple[TABLE_ALIGNMENT, TABLE_ALIGNMENT]] = alignment if alignment is not None else []
    if len(extended_align) > num_columns:
        extended_align = extended_align[0:num_columns]
    elif len(extended_align) < num_columns:
        extended_align += [("^", "<") for i in range(num_columns - len(extended_align))]

    heading_align, cell_align = (x for x in zip(*extended_align))

    field_widths = [len(max(column, key=len)) if len(column) > 0 else 0 for column in columns]
    heading_widths = [max(len(head), 2) for head in headings]
    column_widths = [max(x) for x in zip(field_widths, heading_widths)]

    _ = " | ".join(["{:" + a + str(w) + "}" for a, w in zip(heading_align, column_widths)])
    heading_template = "| " + _ + " |"
    _ = " | ".join(["{:" + a + str(w) + "}" for a, w in zip(cell_align, column_widths)])
    row_template = "| " + _ + " |"

    _ = " | ".join([left_rule[a] + "-" * (w - 2) + right_rule[a] for a, w in zip(cell_align, column_widths)])
    ruling = "| " + _ + " |"

    result = []
    result.append(heading_template.format(*headings).rstrip() + "\n")
    result.append(ruling.rstrip() + "\n")

    for row in zip(*columns):
        result.append(row_template.format(*row).rstrip() + "\n")
    return result


def render_md_table(constants, constant_name) -> str:
    """
    create mark down table
    """
    return get_table(
        headings=[constant_name], records=[[val] for val in getattr(constants, constant_name)], fields=[0]
    )


def get_line_numbers_of_section(file_path: str, heading: str):
    """Get line number of a section. where a section starts with a heading end before next heading"""
    with open(file_path) as fp:
        lines = fp.readlines()
    count = 1
    start = -1
    end = -1
    for line in lines:
        if line.strip("\n") == heading and start < 0:
            start = count
            continue
        if start > 0 and line.startswith("##"):
            end = count
            break
        count = count + 1
    return start, end


def render_markdown_file(constant_names: list):
    table: list[str] = []
    mod = get_constants_module()
    for constant in constant_names:
        table.extend(render_md_table(mod, constant))
        table.append("\n")

    start, end = get_line_numbers_of_section(str(README_PATH), HEADING)

    if start < 0:
        ValueError(f"Heading '{HEADING}' not found in file {str(README_PATH)}")

    new_file = []
    with open(README_PATH) as fp:
        lines = fp.readlines()
        new_file = lines[: start + 1] + table + lines[end:]
    with open(README_PATH, "w") as fp:
        fp.writelines(new_file)


def main(argv: Sequence[str] | None = None):
    render_markdown_file(
        constant_names=["FileLocation", "FileType", "Database"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
