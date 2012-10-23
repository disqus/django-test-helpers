django-test-utils
=================

Test helpers, mostly geared around the database. Tested on Django 1.2.


Install
-------

::

    pip install django-test-utils

Usage
-----

Create a temporary database:

::

    from testutils.dbmanager import TemporaryDatabase

    with TemporaryDatabase(fixtures=['myfixture.json'], db_prefix='test'):
        # do some stuff
        pass

.. note:: If you're using Nashvegas, the TemporaryDatabase will attempt to automatically migrate your database
          and preserve executed migrations.


Run some commands in transactional isolation:

::

    from testutils.dbmanager import Transactionless

    with Transactionless(fixtures=['myfixture.json']):
        # do some stuff
        pass
