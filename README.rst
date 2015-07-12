python-phoenixdb
================

*phoenixdb* is a Python library for accessing the
`Phoenix SQL database <http://phoenix.apache.org/>`_ using the standard
`DB API 2.0 interface <https://www.python.org/dev/peps/pep-0249/>`_. It implements the
`Avatica RPC protocol <http://calcite.incubator.apache.org/docs/avatica.html>`_, which is
used by the `Phoenix query server <http://phoenix.apache.org/server.html>`_, but unfortunately
the protocol is still in development and might change at any time.
The library works with Phoenix 4.4, but will most likely need to be changed to support the
next release. Also note, that this is more of a proof-of-concept implementation, it has not
been thoroughly tested. Use with care.

Example usage::

    import phoenixdb

    with phoenixdb.connect('http://localhost:8765/', autocommit=True) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM test")
        print c.fetchall()

You can also use the database from a Python-based command-line shell::

    virtualenv e
    . e/bin/activate
    pip install -r requirements.txt
    python setup.py develop
    ./examples/shell.py http://localhost:8765/

Running the DB API 2.0 compliance test suite and other unit tests::

    export PHOENIXDB_TEST_DB_URL=http://localhost:8765/
    nosetests

If you need a Phoenix server for experimenting, you can get one running quickly using Vagrant, Ansible and VirtualBox::

    git clone https://bitbucket.org/lalinsky/ansible-hadoop.git
    cd ansible-hadoop
    vagrant up

Known problems:

* "Transaction" support, i.e. non-autocommit mode. Needs support in the Avatica RPC server first. (`CALCITE-767 <https://issues.apache.org/jira/browse/CALCITE-767>`_)
* Proper exception handling, currently it tries to parse the HTML error page it receives from the server. (`CALCITE-645 <https://issues.apache.org/jira/browse/CALCITE-767>`_)
* Can't use TIME/DATE columns. The server returns incomplete data and expects different format on input and output. (`discussion <http://mail-archives.apache.org/mod_mbox/phoenix-user/201506.mbox/%3CCAGUtLj8HDeq7chOSTz%3DVznB-v79%3DCmJ5%3Dt1N9Bbe4wE_m1%3D3zg%40mail.gmail.com%3E>`_)
