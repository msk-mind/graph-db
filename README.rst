========
graph-db
========


.. image:: https://img.shields.io/pypi/v/graph_db.svg
        :target: https://pypi.python.org/pypi/graph_db

.. image:: https://img.shields.io/travis/msk-mind/graph_db.svg
        :target: https://travis-ci.com/msk-mind/graph_db

.. image:: https://readthedocs.org/projects/graph-db/badge/?version=latest
        :target: https://graph-db.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status


A library for the generation of an integrated labeled property graph using neo4j.

This library allows users to import records from various sources like CSV, parquet etc into noe4j and form relationships on the imported records. It is assumed that the user of this library is familiar with neo4j's cypher query language.


* Free software: Apache Software License 2.0
* Documentation: https://graph-db.readthedocs.io.


Test
----

::
    $ make test


Usage
-----
Setup a local neo4j instance with credentials as specified in tests/test_app_config.yml and execute the following commmand.

::
    $ python -m graph_db.builder -d tests/test_data_config.yml -a tests/test_app_config.yml


Features
--------

* Minimal and generic code for loading nodes from various sources (csv, parquet etc.)

* Create hierarchical associations to make a data model that is remarkably similar in its form to the real world

* Normalized yet richly connected entities

* Declarative interface for generating a hierarchical graph database from a set of independent tables

* Ability to easily create many such databases on the fly and not be limited to one. Leads to flexibility and adaptability - especially useful for R&D

* Idempotent graph generation process


Todo
----
- simplify app_config and data_config by removing lists


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
