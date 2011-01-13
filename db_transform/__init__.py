from django.utils.module_loading import module_has_submodule
from django.utils.importlib import import_module
from django.conf import settings

from db_migration.tablespace import ( \
    MigrationDatabaseError, MigrationDatabase)
from db_migration.migration import ( \
    TablespaceMigrationError, TablespaceMigrationRegistry,
    TablespaceMigration)
from db_migration.conversion import ( \
    TablespaceValueConversionError, TablespaceValueConversion,
    SimpleConversion, ConcatinationConversion,
    DynamicSourceConversion, CleanConversion, FlagConversion, BooleanConversion, 
    DateToDateTimeConversion, DateOrNoneConversion,
    ChoiceConversion, MultipleChoiceConversion, MultipleColumnChoiceConversion,)
from db_migration.relation import ( \
    TablespaceRelationBindingError, TablespaceRelationBinding,
    ForeignKeyBinding, ManyToManyBinding,
    RelationBinding, 
    GenericForeignKeyBinding, GenericRelationBinding,)
from db_migration.plan import ( \
    TablespaceMigrationPlan)


def autodiscover():
    """
    Auto-discover ``INSTALLED_APPS``s datamigration modules and fail silently
    when they are not present.

    """
    for app in settings.INSTALLED_APPS:
        mod = import_module(app)
        try:
            import_module("%s.datamigration" % app)
        except:
            if module_has_submodule(mod,'datamigration'):
                raise
