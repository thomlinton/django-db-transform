from django.core.management.base import BaseCommand, CommandError
from django.utils.importlib import import_module
from django.conf import settings

from db_migration.tablespace import MigrationDatabase

from optparse import make_option
import datetime


AVAILABLE_BACKENDS = getattr(settings,'DB_MIGRATION_BACKENDS', {})
DEFAULT_DBNAME = 'migration_db'

class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--dest', action="store", dest="dest", default=None,
                    help="Provide an optional name for the tablespace (otherwise defined by input file)"),
        make_option('--limit', action="store", dest="limit", default=0,
                    help="Provide an optional limit on the number of "+
                         "records imported"),
        make_option('--indexes', action="store", dest="indexes", default=[],
                    help="Specify a comma-delimited list of fields with "+
                         "which to create indices"),
        make_option('--backend', action="store", dest="backend_name", default=None,
                    help="Provide the backend with which to import source data"),
        make_option('--backend-db-name', action="store", dest="backend_db_name", default=DEFAULT_DBNAME,
                    help="Provide a filename to use for the purposes of importing source data"),
        # 
        # TODO: create an 'all' tablespace setting?
        # 
        # make_option('--all' ...
        # 
    )
    help = 'Loads a source migration tablespace.'
    args = "[datafile ...]"

    def handle(self, *datafiles, **options):
        tablespace = options.get('dest')
        limit = long(options.get('limit'))
        indexes = options.get('indexes')

        backend_name = options.get('backend_name')
        db_name = options.get('backend_db_name')

        if not len(datafiles):
            raise CommandError("You must specify a datafile from which to load data.")
        if limit:
            print "Importing %d elements" % (limit)
        if indexes:
            indexes = indexes.split(',')
            print "Installing the following indexes: %s" % (indexes)

        backend_db_name = ''
        if not backend_name:
            try:
                backend_name,backend_db_name = AVAILABLE_BACKENDS['default']
            except KeyError:
                raise CommandError( \
                    u"Please specify a backend to use or define the DB_MIGRATION_BACKENDS project setting.")

        if db_name != DEFAULT_DBNAME or not backend_db_name:
            backend_db_name = db_name
        # backend_db_name = '%s.sqlite3' % (backend_db_name)

        backend_module = None
        try:
            backend_module = import_module(backend_name)
        except ImportError:
            try:
                backend_module = import_module(".%s"%backend_name,package='db_migration.backends')
            except ImportError, e:
                raise CommandError("%s"%e)

        print "Using backend=%s and destination=%s" % (backend_name,backend_db_name)

        for datafile in datafiles:
            if not tablespace:
                tablespace, ext = datafile.rsplit('/',1)[1].split('.')
            print "Loading from %s -> %s" % (datafile,tablespace)

            backend = backend_module.Backend(limit)
            backend.parse(datafile=datafile)
            data = backend.get_data()
            fields = backend.get_fields()

            db = MigrationDatabase(backend_db_name)
            print "Dropping %s" % (tablespace)
            db.delete_tablespace(tablespace)
            print "(Re)loading %s" % (tablespace)
            db.create_tablespace(tablespace,fields)
            db.create_indexes(tablespace,indexes)
            db.load_objects(tablespace,fields,data)
