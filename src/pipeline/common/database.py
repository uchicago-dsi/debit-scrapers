"""Provides database functions for use throughout the Django project."""

# Standard library imports
import contextlib
from collections.abc import Generator, Iterable, Iterator
from io import StringIO
from typing import Any

# Third-party imports
from django.db import connection
from django.db.backends.utils import CursorWrapper


@contextlib.contextmanager
def _setup_teardown_temp_tables(
    cursor: CursorWrapper, dest_table_name: str
) -> Iterator[None]:
    """Context manager for creating and dropping temp tables.

    Args:
        cursor: The database cursor/connection object.

        dest_table_name: The name of the table
            with the schema to use for the temp table.

    Returns:
        `None`
    """
    # Set up context
    cursor.execute(
        f"""
        DROP TABLE IF EXISTS temp;
        CREATE TEMPORARY TABLE temp AS
        SELECT * FROM {dest_table_name} LIMIT 0;
        """  # noqa: S608
    )

    # Execute wrapper function and clean up when finished
    try:
        yield
    finally:
        cursor.execute("DROP TABLE IF EXISTS temp;")


def _create_tsv_file(rows: Iterable) -> StringIO:
    """Reads rows of data to an in-memory file.

    Args:
        rows: The records.

    Returns:
        The file, with the read position set
            to the beginning of the bytes stream.
    """
    file = StringIO()
    for row in rows:
        file.write("\t".join(str(val) for val in row) + "\n")
    file.seek(0)
    return file


def _populate_temp_table(
    cursor: CursorWrapper, records: list[dict], temp_table_col_names: list[str]
) -> None:
    """Populates the temp table with data using a bulk copy command.

    Args:
        cursor: The database cursor/connection object.

        records: The records to write.

        temp_table_col_names: The columns in the temp table,
            which map to fields in the records.

    Returns:
        `None`
    """

    def yield_record_values() -> Generator[list, Any, None]:
        for r in records:
            mapped = [v for k, v in r.items() if k in temp_table_col_names]
            yield mapped

    tsv_file = _create_tsv_file(yield_record_values())
    cursor.copy_from(tsv_file, "temp", columns=tuple(temp_table_col_names), null="None")


def _insert_from_temp_table(
    cursor: CursorWrapper, dest_table_name: str, dest_table_fields: list[dict]
) -> list[int]:
    """Bulk inserts a collection of records into a destination table.

    Args:
        cursor: The database cursor/connection object.

        dest_table_name: The table to copy records to.

        dest_table_fields: The columns of the destination table.

    Returns:
        The newly-created records.
    """
    dest_fields_str = ", ".join(dest_table_fields)
    temp_fields_str = ", ".join(f"temp.{f}" for f in dest_table_fields)
    cursor.execute(
        f"""
        INSERT INTO {dest_table_name} ({dest_fields_str})
        SELECT {temp_fields_str}
        FROM temp
        ON CONFLICT DO NOTHING
        RETURNING *;
        """  # noqa: S608
    )
    return [r[0] for r in cursor.fetchall()]


def _dictfetchall(cursor: CursorWrapper) -> list[dict]:
    """Returns all rows from the cursor.

    Args:
        cursor: The database cursor/connection object.

    Returns:
        The rows.
    """
    desc = cursor.description
    return [dict(zip([col[0] for col in desc], row)) for row in cursor.fetchall()]


def _upsert_from_temp_table(
    cursor: CursorWrapper,
    dest_table_name: str,
    dest_table_fields: list[str],
    unique_constraint_name: str,
    update_table_fields: list[str],
) -> list[dict]:
    """Bulk upserts a collection of records into a destination table.

    Args:
        cursor: The database cursor/connection object.

        dest_table_name: The table to update.

        dest_table_fields: The columns of the
            destination table.

        unique_constraint_name: The name of the destination table's
            unique constraint. Upon a conflict, this will trigger updates
            for the `update_table_fields`.

        update_table_fields: The fields in the destination
            table that should be updated (as opposed to ignored).

    Returns:
        The updated records.
    """
    dest_fields_str = ", ".join(dest_table_fields)
    temp_fields_str = ", ".join(f"temp.{f}" for f in dest_table_fields)
    update_fields_str = ", ".join(update_table_fields)
    excl_update_fields_str = ", ".join(f"excluded.{f}" for f in update_table_fields)
    row_str = "ROW" if len(update_table_fields) == 1 else ""
    cursor.execute(
        f"""
        INSERT INTO {dest_table_name} ({dest_fields_str})
        SELECT {temp_fields_str}
        FROM temp
        ON CONFLICT ON CONSTRAINT {unique_constraint_name} DO UPDATE
        SET ({update_fields_str}) = {row_str}({excl_update_fields_str})
        RETURNING *;
        """  # noqa: S608
    )
    upserted_records = _dictfetchall(cursor)
    return upserted_records


def bulk_insert_records(
    records: list[Any], dest_table_name: str, dest_table_fields: list[str]
) -> list[int]:
    """Performs a bulk insert and returns the newly-created records' ids.

    NOTE: At the time of writing, it is not possible to return ids
    using Django's `bulk_create` method when its `ignore_conflicts`
    parameter is passed an argument of `True`.

    Args:
        records: The records to insert.

        dest_table_name: The table to receive the records.

        dest_table_fields: The columns of the destination table.
            Expected to match the fields of the given records.

    Returns:
        The newly-created records.
    """
    with connection.cursor() as cursor:
        with _setup_teardown_temp_tables(cursor, dest_table_name):
            _populate_temp_table(cursor, records, dest_table_fields)
            return _insert_from_temp_table(cursor, dest_table_name, dest_table_fields)


def bulk_upsert_records(
    records: list[Any],
    dest_table_name: str,
    dest_table_fields: list[str],
    unique_constraint_name: str,
    update_table_fields: list[str],
) -> list[dict]:
    """Performs a bulk upsert and returns the new and/or updated records.

    Args:
        records: The records to upsert.

        dest_table_name: The table to update.

        dest_table_fields: The columns of the destination table.
            Expected to match the fields of the given records.

        unique_constraint_name: The name of the destination table's
            unique constraint. Upon a conflict, this will trigger updates
            for the `update_table_fields`.

        update_table_fields: The fields in the destination
            table that should be updated (as opposed to ignored).

    Returns:
        The updated records.
    """
    with connection.cursor() as cursor:
        with _setup_teardown_temp_tables(cursor, dest_table_name):
            _populate_temp_table(cursor, records, dest_table_fields)
            return _upsert_from_temp_table(
                cursor,
                dest_table_name,
                dest_table_fields,
                unique_constraint_name,
                update_table_fields,
            )
