import os.path
import sys

from django.conf import settings
from django.core.management import call_command
from django.core.management.commands import flush
from django.db import connections, transaction, DEFAULT_DB_ALIAS
from django.db.models import signals
from django.test.testcases import disable_transaction_methods, restore_transaction_methods
from django.test.simple import dependency_ordered

from testhelpers.dbproxy import DatabaseProxy


class Transactionless(object):
    """
    Runs a block of code in transactional isolations.

    If fixtures are provided, they will be loaded, and reloaded
    upon exiting the block.

    >>> with Transactionless():
    >>>     # do something
    >>>     pass
    """
    def __init__(self, fixtures=None):
        self.fixtures = fixtures

    def __enter__(self):
        databases = connections

        for db in databases:
            transaction.enter_transaction_management(using=db)
            transaction.managed(True, using=db)

        disable_transaction_methods()

        if self.fixtures:
            for db in databases:
                if isinstance(connections[db], DatabaseProxy):
                    continue
                call_command('loaddata', *self.fixtures, **dict(verbosity=0, database=db, commit=False))

    def __exit__(self, exc_type, exc_value, traceback):
        databases = connections

        restore_transaction_methods()

        for db in databases:
            transaction.rollback(using=db)
            if transaction.is_managed(using=db):
                transaction.leave_transaction_management(using=db)
            else:
                print >> sys.stderr, "---> Unable to rollback transaction managent on %r" % db

        for connection in connections.all():
            connection.close()


def save_migrations_and_flush(original_func):
    """
    Captures database migrations and re-saves them when the db is flushed.

    This guarantees we maintain our correct state.
    """
    def wrapped(inst, **options):
        from nashvegas.models import Migration

        db = options.get('database', DEFAULT_DB_ALIAS)
        migrations = list(Migration.objects.using(db).all())
        try:
            return original_func(inst, **options)
        finally:
            for migration in migrations:
                migration.save(using=db)
    return wrapped


class TemporaryDatabase(object):
    """
    Manages bootstrapping a testing database and loading global fixtures.

    Supports auto migrating (and saving) nashvegas tables.

    >>> with TemporaryDatabase():
    >>>    # do some stuff
    """
    def __init__(self, automigrate=True, verbosity=1, fixtures=None, db_prefix="test"):
        self.automigrate = automigrate
        self.verbosity = verbosity
        self.fixtures = fixtures
        self.db_prefix = db_prefix

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        self._fixture_state = set()

        # Set the TEST_NAME for all the databases before we start creating them.
        db_prefix = self.db_prefix + '_'
        for k in settings.DATABASES.iterkeys():
            settings.DATABASES[k]['TEST_NAME'] = db_prefix + settings.DATABASES[k]['NAME']

        self.bootstrap()

    def stop(self):
        signals.post_syncdb.disconnect(dispatch_uid="dbmanager_setup_global_fixtures")

    def has_nashvegas(self):
        return 'nashvegas' in settings.INSTALLED_APPS

    def _can_migrate(self, connection, alias):
        if not self.has_nashvegas():
            return False

        if not self.automigrate:
            return False

        from nashvegas.utils import get_all_migrations, get_applied_migrations

        migration_root = os.path.join(settings.DISQUS_PATH, 'migrations')

        test_db_name = connection.settings_dict['TEST_NAME']
        original_db_name = connection.settings_dict["NAME"]

        connection.close()
        connection.settings_dict["NAME"] = test_db_name

        try:
            applied_migrations = get_applied_migrations(databases=[alias])
        except Exception, e:
            # assume db does not exist
            print >> sys.stderr, "---> Hit an error getting applied migrations:"
            print >> sys.stderr, "--->", e
            return False
        finally:
            # We MUST change the database name back otherwise when we attempt to migrate later
            # theres a good chance it will become something like "test_test_FOO"
            connection.close()
            connection.settings_dict["NAME"] = original_db_name

        for db, migrations in get_all_migrations(migration_root, databases=[alias]).iteritems():
            migrations = set(m[1].rsplit('/', 1)[-1] for m in migrations)
            for migration in applied_migrations.get(db, []):
                if migration not in migrations:
                    print >> sys.stderr, "---> Cannot migrate database %r because %r was not found on disk." % (db, migration)
                    return False

        return True

    def _setup_db(self, connection, alias):
        # We only need to setup databases if migrations dont match:
        can_migrate = self._can_migrate(connection, alias)

        if can_migrate:
            original_db_name = connection.settings_dict["NAME"]
            try:
                test_db_name = self._migrate_test_db(connection)
            except Exception, e:
                print >> sys.stderr, "Hit an error while migrating %r. Bootstrapping database database instead." % connection.alias
                print >> sys.stderr, "--->", e
                connection.close()
                connection.settings_dict["NAME"] = original_db_name
                can_migrate = False

        if not can_migrate:
            connection.close()
            test_db_name = connection.creation.create_test_db(self.verbosity, autoclobber=True)

        return test_db_name

    def _migrate_test_db(self, connection):
        """
        Very similar to DatabaseCreation.create_test_db, except that it
        simply migrates the db (assumes you have this handled) by running
        syncdb and loading fixtures.
        """

        # HACK: this is how we support the readonly-backend
        if hasattr(connection.creation, 'migrate_test_db'):
            return connection.creation.migrate_test_db()

        if self.verbosity >= 1:
            print >> sys.stderr, "---> Migrating test database '%s'..." % connection.alias

        test_db_name = connection.settings_dict['TEST_NAME']

        connection.close()
        connection.settings_dict["NAME"] = test_db_name
        can_rollback = connection.creation._rollback_works()
        connection.settings_dict["SUPPORTS_TRANSACTIONS"] = can_rollback

        call_command('syncdb',
            verbosity=self.verbosity,
            interactive=False,
            database=connection.alias,
            load_initial_data=False)

        # We need to then do a flush to ensure that any data installed by
        # custom SQL has been removed. The only test data should come from
        # test fixtures, or autogenerated from post_syncdb triggers.
        # This has the side effect of loading initial data (which was
        # intentionally skipped in the syncdb).
        call_command('flush',
            verbosity=self.verbosity,
            interactive=False,
            database=connection.alias)

        # Get a cursor (even though we don't need one yet). This has
        # the side effect of initializing the test database.
        connection.cursor()

        return test_db_name

    def _get_databases(self):
        for db in connections:
            if db.endswith('readonly'):
                continue
            if not isinstance(connections[db], DatabaseProxy):
                yield db

    def bootstrap(self):
        # actually setup the database
        self.setup_databases()

        # Monkey patch the ``flush()`` command so we reset fixture state on flush
        def reset_fixture_state_and_flush(original_func):
            def wrapped(inst, **options):
                db = options.get('database', DEFAULT_DB_ALIAS)
                try:
                    self._fixture_state.remove(db)
                except:
                    pass

                # Tasks need to be enabled during flushes
                settings.DISABLE_TASKS = False
                return original_func(inst, **options)

            return wrapped

        flush.Command.handle_noargs = reset_fixture_state_and_flush(flush.Command.handle_noargs)

        # commit changes up to this point and setup global fixtures
        for db in self._get_databases():
            transaction.commit_unless_managed(using=db)
            self.setup_global_fixtures(db)

        # Connect the signal for global fixtures last
        signals.post_syncdb.connect(self.setup_global_fixtures, dispatch_uid="dbmanager_setup_global_fixtures")

    def setup_databases(self):
        """
        Reimplement setup_databases but install our ``DatabaseProxy`` on anything
        that uses TEST_MIRROR.
        """
        # First pass -- work out which databases actually need to be created,
        # and which ones are test mirrors or duplicate entries in DATABASES
        mirrored_aliases = {}
        test_databases = {}
        dependencies = {}
        for alias in connections:
            connection = connections[alias]
            if connection.settings_dict['TEST_MIRROR']:
                # If the database is marked as a test mirror, save
                # the alias.
                mirror_alias = connection.settings_dict['TEST_MIRROR']
                mirrored_aliases[alias] = mirror_alias
                connections._connections[alias] = DatabaseProxy(connections[mirror_alias], alias)
            else:
                # Store a tuple with DB parameters that uniquely identify it.
                # If we have two aliases with the same values for that tuple,
                # we only need to create the test database once.
                item = test_databases.setdefault(
                    connection.creation.test_db_signature(),
                    (connection.settings_dict['NAME'], [])
                )
                item[1].append(alias)

                if 'TEST_DEPENDENCIES' in connection.settings_dict:
                    dependencies[alias] = connection.settings_dict['TEST_DEPENDENCIES']
                else:
                    if alias != DEFAULT_DB_ALIAS:
                        dependencies[alias] = connection.settings_dict.get('TEST_DEPENDENCIES', [DEFAULT_DB_ALIAS])

        if self.has_nashvegas():
            # Monkey patch the ``flush()`` command so we can backup migrations before it's run
            flush.Command.handle_noargs = save_migrations_and_flush(flush.Command.handle_noargs)

        # Second pass -- actually create the databases.
        for signature, (db_name, aliases) in dependency_ordered(test_databases.items(), dependencies):
            alias = aliases[0]

            # Actually create the database for the first connection
            connection = connections[alias]

            test_db_name = self._setup_db(connection, alias)

            # Handle child databases (which may just be proxies)
            for alias in aliases[1:]:
                connection = connections[alias]
                connection.settings_dict['NAME'] = test_db_name

        for alias, mirror_alias in mirrored_aliases.items():
            connections[alias].settings_dict['NAME'] = connections[mirror_alias].settings_dict['NAME']

    def setup_global_fixtures(self, db, **kwargs):
        if not self.fixtures:
            return

        if isinstance(connections[db], DatabaseProxy):
            return

        if db in self._fixture_state:
            return

        self._fixture_state.add(db)

        cursor = connections[db].cursor()
        cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
        call_command('loaddata', *self.fixtures, verbosity=self.verbosity, database=db)
