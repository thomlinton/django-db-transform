import logging
import sqlite3
import re


#
# Module-level SQL query statements
#

#
# TODO: Cleanup/refactor/&c.
#
COLUMN_DEFINITION = u"'%s' TEXT"
CREATE_TABLE = u"CREATE TABLE IF NOT EXISTS %(table_name)s (%(columns)s)"
DROP_TABLE = u"DROP TABLE IF EXISTS %(table_name)s"
CREATE_INDEX = u"CREATE INDEX IF NOT EXISTS %(index_name)s ON %(table_name)s (%(columns)s)"
INSERT_STMT = u"INSERT INTO %(table_name)s (%(columns)s) VALUES (%(values)s)"
JOIN_CLAUSE = u"%(join_type)s JOIN %(table_name)s ON (%(lhs_table)s.%(lhs_col)s=%(rhs_table)s.%(rhs_col)s)"
WHERE_CLAUSE = u"WHERE %(conditions)s"
SELECT_STMT = u"SELECT * FROM %(table_name)s %(join_clause)s %(where_clause)s"
SELECT_DISTINCT_STMT = u"SELECT DISTINCT * FROM %(table_name)s %(join_clause)s %(where_clause)s"


class MigrationDatabaseError(Exception):
    pass

class MigrationDatabase(object):
    """
    Provides an interface for a transition database: one which with a (partially, at least)
    defined schema and one which is type-less.

    """
    def __init__(self, migration_db_name, writeback=True):
        self.con = sqlite3.connect( "%s.sqlite3" % migration_db_name )
        self.con.row_factory = sqlite3.Row

    def create_tablespace(self, name, fields):
        columns_definition_statement = u", ".join([ COLUMN_DEFINITION%(field[0]) for field in fields])
        create_table_statement = CREATE_TABLE % {'table_name':name, 'columns':columns_definition_statement}
        logging.info("NOTICE: create_table_statement=%s" % (create_table_statement))
        self.con.execute( create_table_statement )
        self.con.commit()

        logging.info("Created tablespace %s" % (name))

    def delete_tablespace(self, name):
        drop_table_statement = DROP_TABLE % {'table_name':name}
        self.con.execute( drop_table_statement )
        self.con.commit()

        logging.info("Deleted tablespace %s" % (name))

    def create_indexes(self, tablespace, indexes):
        for index in indexes:
            create_index_statement = CREATE_INDEX % {
                'index_name':"%s__%s"%(tablespace,index),
                'table_name':tablespace,
                'columns':index,
                }
            self.con.execute( create_index_statement )
        self.con.commit()

        logging.warn("Loaded indexes %s into tablespace %s" % (indexes,tablespace))
        return indexes

    def load_objects(self, tablespace, fields, data):
        fields_ = fields
        fields = [u"'%s'"%field[0] for field in fields_]
        field_markers = [u":%s"%field[0] for field in fields_]

        for datum in data:
            insert_statement = INSERT_STMT % {
                'table_name': tablespace,
                'columns': ','.join(fields),
                'values': ','.join(['?' for key,val in datum.iteritems()]),
                }
            self.con.execute( insert_statement, tuple([datum[field[0]] for field in fields_]) )
        self.con.commit()
        logging.info("Serialized %d records" % (len(data)))

    def _get_select_statement(self, tablespace, params, additional_tablespaces, **options):
        # print "MigrationDatabase.get_object(s): self.tablespace=%s, params=%s, self.additional_tablespaces=%s" % \
        #     (tablespace,params,additional_tablespaces)
        unqiue = options.pop('unique',False)

        where_clause = ''
        conditions = ' AND '.join([ '%s=:%s'%(key,key) for key,val in params.iteritems()])
        if conditions:
            where_clause = WHERE_CLAUSE % {'conditions':conditions}

        join_clause = ''
        join_clauses = []
        for tablespace_name,col_mapping in additional_tablespaces.iteritems():
            # set default join type if not specified
            join_type = 'LEFT'
            try:
                join_type = col_mapping[2]
            except IndexError:
                pass
            join_clauses.append( \
                JOIN_CLAUSE % {
                    'join_type':join_type, 'table_name':tablespace_name, 
                    'lhs_col':col_mapping[0], 'rhs_col':col_mapping[1],
                    'lhs_table':tablespace, 'rhs_table':tablespace_name,
                    })
        join_clause = ' '.join(join_clauses)

        select_params = {                
            'table_name':tablespace,
            'where_clause':where_clause,
            'join_clause': join_clause
            }

        select_statement = SELECT_STMT % select_params
        return (select_statement,params)

    def get_object(self, tablespace, lookup, additional_tablespaces, **options):
        cursor = self.con.cursor()
        select_statement,lookup = \
            self._get_select_statement(tablespace,lookup,additional_tablespaces,**options)

        print "Running: %s (lookup: %s)" % (select_statement,lookup)
        cursor.execute( select_statement, lookup )
        return cursor.fetchone()

    def get_objects(self, tablespace, conditions, additional_tablespaces, **options):
        cursor = self.con.cursor()
        select_statement, conditions = \
            self._get_select_statement(tablespace,conditions,additional_tablespaces,**options)

        print "Running: %s (conditions: %s)" % (select_statement,conditions)
        cursor.execute( select_statement, conditions )
        return cursor.fetchall()
