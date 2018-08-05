#!/usr/bin/env python
# encoding: utf-8


def upsert(db, table, fields, object_list):
    cursor = db.cursor()
    table = "`"+db.escape_string(table)+"`"
    fields = ["`"+db.escape_string(field)+"`" for field in fields]
    placeholders = ["%s" for field in fields]
    assignments = ["{x} = VALUES({x})".format(
        x=db.escape_string(x)
    ) for x in fields]

    query_string = """INSERT INTO
    {table}
    ({fields})
    VALUES
    ({placeholders})
    ON DUPLICATE KEY UPDATE {assignments}"""

    cursor.executemany(query_string.format(
        table=table,
        fields=", ".join(fields),
        placeholders=", ".join(placeholders),
        assignments=", ".join(assignments)
    ), object_list)
    db.commit()
