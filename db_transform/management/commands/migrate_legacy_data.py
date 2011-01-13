from django.core.management.base import BaseCommand, CommandError

from db_migration.plan import TablespaceMigrationPlan
from db_migration import autodiscover

from optparse import make_option


class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('--plan', action="store", dest='plan', default='',
            help='Specify a migration plan to execute'),
        make_option('--migration', action="store", dest='migration', default='',
            help='Specify a particular migration'),
        make_option('--group', action="store", dest='group', default='',
            help='Specify a particular migration group'),
        make_option('--limit', action="store", dest='limit', default=0,
            help='Integer argument to limit records listed'),
    )
    help = 'Transforms source migration data stored in the given tablespace(s)'

    def handle(self, **options):
        plan = options.get('plan')
        migration = options.get('migration')
        group = options.get('group')
        if group and not plan:
            raise CommandError( \
                u"You must supply a migration plan when specifying `group`")

        limit = options.get('limit')
        try:
            limit = long(limit)
        except ValueError:
            raise CommandError( \
                u"Supplied value for `limit` is not a valid integer.")

        autodiscover()

        # open the plan if provided
        plan = TablespaceMigrationPlan( \
            planfile=plan,groupname=group)

        # otherwise, add specified migration
        if plan.is_empty() and migration:
            plan.add_migration( migration )

        # activate the migration plan
        plan.run()
