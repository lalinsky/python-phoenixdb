#!/usr/bin/env python

import phoenixdb

with phoenixdb.connect('http://localhost:8765/', autocommit=True) as connection:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS test")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, text VARCHAR)")
        cursor.executemany("UPSERT INTO test VALUES (?, ?)", [[1, 'hello'], [2, 'world']])
        cursor.execute("SELECT * FROM test ORDER BY id")
        for row in cursor:
            print(row)
