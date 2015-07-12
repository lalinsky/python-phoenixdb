Phoenix database adapter for Python
===================================

``phoenixdb`` is a Python library for accessing the
`Phoenix SQL database <http://phoenix.apache.org/>`_
using the
`remote query server <http://phoenix.apache.org/server.html>`_ introduced
in Phoenix 4.4.  The library implements the  
standard `DB API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_ interface,
which should be familiar to most Python programmers.

Example usage::

    import phoenixdb

    database_url = 'http://localhost:8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
    cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
    cursor.execute("SELECT * FROM users")
    print cursor.fetchall()
