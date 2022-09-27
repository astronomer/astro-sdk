from astro.files import File
from astro.sql.table import Table, TempTable, Metadata


def serialize(input: Table | File) -> dict:
    if isinstance(input, Table) or isinstance(input, TempTable):
        return {
            "class": "Table",
            "name": input.name,
            "metadata": {
                "schema": input.metadata.schema,
                "database": input.metadata.database
            },
            "temp": input.temp,
            "conn_id": input.conn_id
        }
    else:
        return input


def deserialize(input: dict):
    if not isinstance(input, dict) or not input.get("class") or input["class"] not in ["Table"]:
        return input
    if input["class"] == "Table":
        return Table(
            name=input["name"],
            metadata=Metadata(
                **input["metadata"]
            ),
            temp=input["temp"],
            conn_id=input["conn_id"]
        )
    return input