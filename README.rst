django-test-helpers
===================

Test helpers, mostly geared around the database. Tested on Django 1.2.


Install
-------

::

    pip install django-test-helpers

Usage
-----

Create a temporary database:

::

    from testhelpers.dbmanager import TemporaryDatabase

    with TemporaryDatabase(fixtures=['myfixture.json'], db_prefix='test'):
        # do some stuff
        pass

.. note:: If you're using Nashvegas, the TemporaryDatabase will attempt to automatically migrate your database
          and preserve executed migrations.


Run some commands in transactional isolation:

::

    from testhelpers.dbmanager import Transactionless

    with Transactionless(fixtures=['myfixture.json']):
        # do some stuff
        pass
