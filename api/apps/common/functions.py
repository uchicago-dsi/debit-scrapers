"""Functions available to all apps.
"""

import contextlib
from django.db import connection
from django.db.backends.utils import CursorWrapper
from io import StringIO
from typing import Any, Dict, Iterable, List


@contextlib.contextmanager
def _setup_teardown_temp_tables(
    cursor: CursorWrapper,
    dest_table_name: str) -> None:
    """Context manager for creating and dropping temp tables.

    Args:
        cursor (`CursorWrapper`): The database cursor/connection object.

        dest_table_name (`str`): The name of the table
            with the schema to use for the temp table.

    Returns:
        `None`
    """
    cursor.execute(
       f"""
        DROP TABLE IF EXISTS temp;

        CREATE TEMPORARY TABLE temp AS
        SELECT * FROM {dest_table_name} LIMIT 0;
        """
    )
    try:
        yield
    finally:
        cursor.execute(
            """
            DROP TABLE IF EXISTS temp;
            """
        )

def _create_tsv_file(rows: Iterable) -> StringIO:
    """Reads rows of data to an in-memory file.

    Args:
        rows (`iterable`): The records.

    Returns:
        (`io.StringIO`): The file, with the read
            position set to the beginning of
            the bytes stream.
    """
    file = StringIO()
    for row in rows:
        file.write("\t".join(str(val) for val in row) + "\n")
    file.seek(0)
    return file

def _populate_temp_table(
    cursor: CursorWrapper,
    records: List[Dict],
    temp_table_col_names: List[str]) -> None:
    """Populates the temp table with data using a bulk copy command.

    Args:
        cursor (`CursorWrapper`): The database cursor/connection object.

        records (`list` of `dict`): The records to write.

        temp_table_col_names (`list` of `str`): The columns
            in the temp table, which map to fields in the records.

    Returns:
        `None`
    """
    def yield_record_values():
        for r in records:
            mapped = [v for k, v in r.items() if k in temp_table_col_names]
            yield mapped

    tsv_file = _create_tsv_file(yield_record_values())
    cursor.copy_from(
        tsv_file,
        "temp",
        columns=tuple(temp_table_col_names),
        null="None"
    )

def _insert_from_temp_table(
    cursor: CursorWrapper,
    dest_table_name: str,
    dest_table_fields: List[str]) -> List[int]:
    """Bulk inserts a collection of records into a destination table.

    Args:
        cursor (`CursorWrapper`): The database cursor/connection object.

        dest_table_name (`str`): The table to copy records to.

        dest_table_fields (`list` of `str`): The columns of the
            destination table.

    Returns:
        (`list` of `int`): The ids of the newly-created records.
    """
    dest_fields_str = ", ".join(dest_table_fields)
    temp_fields_str = ", ".join(f"temp.{f}" for f in dest_table_fields)
    cursor.execute(
        f"""
        INSERT INTO {dest_table_name} ({dest_fields_str})
        SELECT {temp_fields_str}
        FROM temp
        ON CONFLICT DO NOTHING
        RETURNING id;
        """
    )
    return [r[0] for r in cursor.fetchall()]

def _dictfetchall(cursor: CursorWrapper) -> List[Dict]: 
    """Returns all rows from the cursor.

    Args:
        cursor (`CursorWrapper`): The database cursor/connection object.

    Returns:
        (`list` of `dict`): The rows.
    """
    desc = cursor.description 
    return [
        dict(zip([col[0] for col in desc], row)) 
        for row in cursor.fetchall() 
    ]

def _upsert_from_temp_table(
    cursor: CursorWrapper,
    dest_table_name: str,
    dest_table_fields: List[str],
    unique_constraint_name: str,
    update_table_fields: List[str]) -> List[Dict]:
    """Bulk upserts a collection of records into a destination table.

    Args:
        cursor (`CursorWrapper`): The database cursor/connection object.
            
        dest_table_name (`str`): The table to update.

        dest_table_fields (`list` of `str`): The columns of the
            destination table.

        unique_constraint_name (`str`): The name of the destination table's
            unique constraint. Upon a conflict, this will trigger updates
            for the `update_table_fields`.

        update_table_fields (`list` of `str`): The fields in the destination
            table that should be updated (as opposed to ignored).

    Returns:
        (`list` of `dict`): The updated records.
    """
    dest_fields_str = ", ".join(dest_table_fields)
    temp_fields_str = ", ".join(f"temp.{f}" for f in dest_table_fields)
    update_fields_str = ", ".join(update_table_fields)
    excl_update_fields_str = ", ".join(
        f"excluded.{f}" 
        for f in update_table_fields
    )
    row_str = "ROW" if len(update_table_fields) == 1 else ""
    cursor.execute(
        f"""
        INSERT INTO {dest_table_name} ({dest_fields_str})
        SELECT {temp_fields_str}
        FROM temp
        ON CONFLICT ON CONSTRAINT {unique_constraint_name} DO UPDATE
        SET ({update_fields_str}) = {row_str}({excl_update_fields_str})
        RETURNING *;
        """
    )
    upserted_records = _dictfetchall(cursor)
    return upserted_records

def bulk_insert_records(
    records: List[Any],
    dest_table_name: str,
    dest_table_fields: List[str]) -> List[int]:
    """Bulk inserts records into a database table while ignoring
    conflicts and then returns the list of newly-generated record ids.
    NOTE: At the time of writing, it is not possible to return ids
    using Django's `bulk_create` method when its `ignore_conflicts`
    parameter is passed an argument of `True`.
    
    Args:
        records (`list` of `dict`): The records to insert.
            
        dest_table_name (`str`): The table to receive the records.

        dest_table_fields (`list` of `str`): The columns of the
            destination table. It is expected to match the fields
            of the given records.

    Returns:
        (`list` of `int`): The ids.
    """
    with connection.cursor() as cursor:
        with _setup_teardown_temp_tables(cursor, dest_table_name):
            _populate_temp_table(cursor, records, dest_table_fields)
            return _insert_from_temp_table(
                cursor, 
                dest_table_name, 
                dest_table_fields
            )

def bulk_upsert_records(
    records: List[Any],
    dest_table_name: str,
    dest_table_fields: List[str],
    unique_constraint_name: str,
    update_table_fields: List[str]) -> List[Dict]:
    """Bulk upserts a collection of records into a destination table
    and returns the updated representations.

    Args:
        cursor (`CursorWrapper`): The database cursor/connection object.
            
        dest_table_name (`str`): The table to update.

        dest_table_fields (`list` of `str`): The columns of the
            destination table.

        unique_constraint_name (`str`): The name of the destination table's
            unique constraint. Upon a conflict, this will trigger updates
            for the `update_table_fields`.

        update_table_fields (`list` of `str`): The fields in the destination
            table that should be updated (as opposed to ignored).

    Returns:
        (`list` of `dict`): The updated records.
    """ 
    with connection.cursor() as cursor:
        with _setup_teardown_temp_tables(cursor, dest_table_name):
            _populate_temp_table(cursor, records, dest_table_fields)
            return _upsert_from_temp_table(
                cursor,
                dest_table_name,
                dest_table_fields,
                unique_constraint_name,
                update_table_fields
            )
