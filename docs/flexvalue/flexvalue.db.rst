================
``flexvalue.db``
================

.. automodule:: flexvalue.db

   .. contents::
      :local:

.. currentmodule:: flexvalue.db


Functions
=========

- :py:class:`DBManager`:
  The base class for managing database interactions.

- :py:class:`PostgresqlManager`:
  Implementation of DBManager for PostgreSQL databases.

- :py:class:`BigQueryManager`:
  Implementation of DBManager for Google BigQuery databases.


.. autoclass:: DBManager

.. autoclass:: PostgresqlManager(DBManager)

.. autoclass:: BigQueryManager(DBManager)
