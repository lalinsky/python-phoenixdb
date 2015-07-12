Phoenix database adapter for Python
===================================

``phoenixdb`` is a Python library for accessing the
`Phoenix SQL database <http://phoenix.apache.org/>`_
using the
`remote query server <http://phoenix.apache.org/server.html>`_ introduced
in Phoenix 4.4.  The library implements the  
standard `DB API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_ interface,
which should be familiar to most Python programmers.

Installation
------------

The easiest way to install the library is using `pip <https://pip.pypa.io/en/stable/>`_::

    pip install phoenixdb

You can also download the source code from `Bitbucket <https://bitbucket.org/lalinsky/python-phoenixdb/downloads>`_,
extract the archive and then install it manually::

    cd /path/to/python-phoenix-x.y.z/
    python setup.py install

Usage
-----

The library implements the standard DB API 2.0 interface, so it can be
used the same way you would use any other SQL database from Python, for example::

    import phoenixdb

    database_url = 'http://localhost:8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
    cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
    cursor.execute("SELECT * FROM users")
    print cursor.fetchall()

Setting up a development environment
------------------------------------

If you want to quickly try out the included examples, you can set up a
local `virtualenv <https://virtualenv.pypa.io/en/latest/>`_ with all the
necessary requirements::

    virtualenv e
    source e/bin/activate
    pip install -r requirements.txt
    python setup.py develop

If you need a Phoenix server for experimenting, you can get one running
quickly using Vagrant, Ansible and VirtualBox::

    git clone https://bitbucket.org/lalinsky/ansible-hadoop.git
    cd ansible-hadoop
    vagrant up

Running the test suite
----------------------

The library comes with a test suite for testing Python DB API 2.0 compliance and
various Phoenix-specific features. In order to run the test suite, you need a
working Phoenix database and set the ``PHOENIXDB_TEST_DB_URL`` environment variable::

    export PHOENIXDB_TEST_DB_URL='http://localhost:8765/?v=1.4.0'
    nosetests

Interactive SQL shell
---------------------

There is a Python-based interactive shell include in the examples folder, which can be
used to connect to Phoenix and execute queries::

    ./examples/shell.py http://localhost:8765/

Known issues
------------

- In general, the library has not been battle-tested yet. You might encounter almost any problem. Use with care.
- You can only use the library in autocommit mode. The native Java Phoenix library also implements batched upserts, which can be committed at once, but this is not exposed over the remote server.
  (`CALCITE-767 <https://issues.apache.org/jira/browse/CALCITE-767>`_)
- In some cases, generic exceptions are raises, instead of more specific SQL errors. This is because the Avatica server from Calcite currently does not pass errors in a structured format.
- BINARY, TIME, DATE and TIMESTAMP data types do not work with Phoenix 4.4, which ships with Calcite 1.2.
  You can use them if you make a custom build of Phoenix with a more recent version of Calcite.
  (`CALCITE-671 <https://issues.apache.org/jira/browse/CALCITE-671>`_)
- Requests with more than 16k data will fail on Phoenix 4.4.
  (`CALCITE-780 <https://issues.apache.org/jira/browse/CALCITE-780>`_)
- TIME and DATE columns in Phoenix are stored as full timestamps with a millisecond accuracy,
  but the remote protocol only exposes the time (hour/minute/second) or date (year/month/day)
  parts of the columns. (`CALCITE-797 <https://issues.apache.org/jira/browse/CALCITE-797>`_, `CALCITE-798 <https://issues.apache.org/jira/browse/CALCITE-798>`_)
- TIMESTAMP columns in Phoenix are stored with a nanosecond accuracy, but the remote protocol truncates them to milliseconds. (`CALCITE-796 <https://issues.apache.org/jira/browse/CALCITE-796>`_)

API Reference
-------------

.. toctree::
   :maxdepth: 2

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _
