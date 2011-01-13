from db_migration.migration import TablespaceMigrationRegistry

import ConfigParser
import logging


class TablespaceMigrationPlanError(Exception):
    pass

class TablespaceMigrationPlan(object):
    """
    

    """
    def __init__(self, planfile='', groupname=''):
        self.migrations = []
        if planfile:
            plan = ConfigParser.SafeConfigParser()
            plan.read( planfile )
            for group in plan.get('plan','groups').split():
                if groupname and group != groupname:
                    continue
                for name in plan.get(group,'migrations').split():
                    self.add_migration( name )

    def is_empty(self):
        return len(self.migrations) == 0

    def add_migration(self, migration_name):
        migration_cls = \
            TablespaceMigrationRegistry.get_migration(migration_name)
        self.migrations.append( migration_cls )

    def run(self):
        for migration_cls in self.migrations:
            logging.info( "Running %s" % (migration_cls.__name__) )
            migration = migration_cls()
            migration.handle()
