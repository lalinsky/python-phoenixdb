Welcome to phoenixdb's documentation!
=====================================

`phoenixdb` is a Python library for accessing the
`Phoenix SQL database <http://phoenix.apache.org/>`_ using the standard
`DB API 2.0 interface <https://www.python.org/dev/peps/pep-0249/>`_. It implements the
`Avatica RPC protocol <http://calcite.incubator.apache.org/docs/avatica.html>`_, which is
used by the `Phoenix query server <http://phoenix.apache.org/server.html>`_, but unfortunately
the protocol is still in development and might change at any time.
The library works with Phoenix 4.4, but will most likely need to be changed to support the
next release. Also note, that this is more of a proof-of-concept implementation, it has not
been thoroughly tested. Use with care.

Contents:

.. toctree::
   :maxdepth: 2

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _
