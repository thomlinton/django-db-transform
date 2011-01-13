from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction, IntegrityError
from django.forms.models import modelform_factory
from django.db.models.query import QuerySet
from django.conf import settings

from db_migration.tablespace import MigrationDatabase
from db_migration.conversion import ( \
    TablespaceValueConversion, SimpleConversion, ConcatinationConversion)

import logging
import copy


class TablespaceMigrationError(Exception):
    pass

class TablespaceMigrationOptions(object):
    backend = 'default'
    tablespace = ''
    additional_tablespaces = tuple()
    conditions = tuple()
    
    presave_field_map = {}
    postsave_field_map = {}
    presave_relation_map = {}
    postsave_relation_map = {}

    model = None
    form = None
    queryset = None

    dependent_models = []
    update = False
    lookup = {}
    defaults = {}

    def __init__(self, opts):
        if opts:
            for key,value in opts.__dict__.iteritems():
                setattr(self, key, value)

class TablespaceMigrationBase(type):
    """ """
    def __new__(cls, name, bases, attrs):
        super_new = super(TablespaceMigrationBase, cls).__new__
        parents = [b for b in bases if isinstance(b, TablespaceMigrationBase)]
        if not parents:
            # If this isn't a subclass of TablespaceMigration, don't do anything special.
            return super_new(cls, name, bases, attrs)

        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta

        meta = TablespaceMigrationOptions(meta)

        for parent in parents:
            base_meta = getattr(parent,"_meta",None)
            if base_meta and not meta:
                meta = base_meta
            elif base_meta:
                for key,val in base_meta.__dict__.iteritems():
                    try:
                        child_val = getattr(meta,key)
                        if child_val and type(val) == list:
                            new_val = copy.deepcopy( val )
                            new_val.extend( child_val )
                            setattr(meta,key,new_val)
                        elif child_val and type(val) == dict:
                            new_val = copy.deepcopy( val )
                            new_val.update( child_val )
                            setattr(meta,key,new_val)
                        elif not child_val:
                            setattr(meta,key,val)
                    except AttributeError, e:
                        raise TablespaceMigrationError( \
                            u"Attribute %s is unset: %s" % (key,str(e)))

        setattr(new_class, '_meta', meta)
        return new_class

class TablespaceMigration(object):
    """ """
    __metaclass__ = TablespaceMigrationBase

    # def __init__(self, tablespace=None, dependent_migration=False):
    def __init__(self, tablespace=None):
        if not self._meta.model:
            raise TablespaceMigrationError( \
                u"`model` must be specified on the Meta innerclass (%s)" % (self))

        # self.dependent_migration = dependent_migration
        self.dependent_models = self._meta.dependent_models

        self.backend = self._meta.backend
        self.tablespace = self._meta.tablespace
        # if not self._meta.tablespace or not tablespace:
        #     raise TablespaceMigrationError( \
        #         u"`tablespace` must be specified on the Meta innerclass (%s)" % (self))
        if tablespace:
            self.tablespace = tablespace

        self.additional_tablespaces = dict(self._meta.additional_tablespaces)
        self.conditions = dict(self._meta.conditions)

        self.presave_field_map = self._meta.presave_field_map
        self.postsave_field_map = self._meta.postsave_field_map
        self.presave_relation_map = self._meta.presave_relation_map
        self.postsave_relation_map = self._meta.postsave_relation_map

        self.model_cls = self._meta.model
        if self._meta.form:
            self.form_cls = self._meta.form
        else:
            self.form_cls = modelform_factory( self.model_cls )
        self.queryset = self._meta.queryset
        self.update = self._meta.update
        self.lookup = self._meta.lookup
        self.defaults = self._meta.defaults

        available_backends = {}
        try:
            available_backends = settings.DB_MIGRATION_BACKENDS
        except AttributeError, e:
            raise TablespaceMigrationError("%s"%e)
        db_name = ''
        try:
            backend,db_name = available_backends[self._meta.backend]
        except KeyError, e:
            raise TablespaceMigrationError("%s"%e)

        self.db = MigrationDatabase(db_name)

    def get_raw_object(self, lookup):
        """
        Fetches the object from the migration source (if it exists) given by ``lookup``.

        """
        return self.db.get_object( \
            self.tablespace,lookup,self.additional_tablespaces)

    def get_object(self, raw_object, extra_lookup={}):
        """
        Fetches the object from the migration destination (if it exists).

        """
        if not self.lookup and not extra_lookup:
            return None

        lookup = self.lookup.copy()
        lookup.update( extra_lookup )

        obj_attrs = {}
        for key,value in lookup.iteritems():
            obj_attrs[key] = value

        #
        # TODO: Complete lookup population
        # 
        # for item in lookup.iteritems():
        #     if type(value) == tuple:
        #         key,value = item
        #         obj_attrs[key] = value
        #     elif type(item) == str or type(item) == unicode:
        #         key = item
        #         if key in self.presave_field_map.keys():
        #             convertor,conversion_value = \
        #                 self.process_field(key,self.presave_field_map[key])
        #             obj_attrs[key] = convertor.convert(conversion_value,raw_object,None)
        #         elif key in self.presave_relation_map.keys():
        #             for (relation,related_obj) in self.process_relation(key,self.presave_relation_map[key],raw_object):
        #                 if related_obj:
        #                     relation.add_to_lookup( obj_attrs, key, related_obj )

        try:
            return self.model_cls.objects.get(**lookup)
        except ObjectDoesNotExist, e:
            logging.warn( \
                "%(class_name)s get_object: %(error)s" % \
                    {'class_name':self.__class__.__name__,
                     'error':'%s'%e})
        return None

    def process_field(self, key, value, raw_object):
        """ """
        convertor = None
        conversion_value = value

        # 'somekey': 'someotherfield'
        if type(value) == str or type(value) == unicode:
            convertor = SimpleConversion(field_name=value)
        # 'somekey': ('fieldone','fieldtwo')
        elif type(value) == tuple:
            convertor = ConcatinationConversion()
        # 'somekey': SomeConversion (instance)
        elif isinstance(value,TablespaceValueConversion):
            convertor = value
        # 'somekey': SomeConversionCls
        else:
            try:
                if issubclass(value,TablespaceValueConversion):
                    convertor = value()
            except TypeError:
                pass

        if convertor.field_name is not None:
            conversion_value = raw_object[convertor.field_name]
        else:
            logging.warn( \
                "%s.field_name explicitly unset. Settings converted_value to None." % \
                    (convertor.__class__.__name__))

        return (convertor,conversion_value)

    def process_relation(self, key, relations, raw_object, instance=None):
        """ """
        if type(relations) != tuple and type(relations) != list:
            relations = [relations,]

        objs = []
        for relation_cls in relations:
            relation = relation_cls(parent_migration=self)
            objs.append( (relation,relation.handle(raw_object,instance)) )

        return objs

    @transaction.commit_on_success()
    def migrate_object(self, raw_object, instance=None, initial={}):
        """ """
        form_data = self.defaults.copy()
        form_data.update( initial )

        if not instance:
            instance = self.get_object(raw_object)

        # 
        # PRE-SAVE FIELDS
        #
        for key, value in self.presave_field_map.iteritems():
            logging.info("presave_field: %s:%s" % (key,value))            
            if value is None:
                logging.info("key=%s has been explicitly removed in tablespace=%s. Skipping." % \
                                 (key,self.tablespace))
                continue

            try:
                #
                # TODO: remove `form_data` argument from Conversion objects?
                #
                convertor,conversion_value = self.process_field(key,value,raw_object)
                form_data[key] = convertor.convert(conversion_value,raw_object,form_data)
            except IndexError, e:
                logging.info("Unable to index key=%s in form or value=%s in tablespace=%s. Skipping." % \
                                 (key,value,self.tablespace))

        #
        # PRE-SAVE RELATIONS
        # 
        for key, relations in self.presave_relation_map.iteritems():
            logging.info("presave_relation: %s:%s" % (key,relations))
            for (relation,related_obj) in self.process_relation(key,relations,raw_object):
                if related_obj:
                    relation.add_to_form( form_data, key, related_obj )
                else:
                    logging.warn("Related object of type=%s not created from %s" % \
                                     (relation.__class__,dict(raw_object)))

        #
        # FORM POPULATION
        #
        f = self.form_cls(form_data, instance=instance)
        try:
            if f.is_valid():
                try:
                    instance = f.save()
                except Exception, e:
                    logging.warn( "Error in instantiation: %s" % (str(e)) )
            else:
                logging.warn( "Error in object creation: %s" % (f.errors) )
                return
        except IntegrityError, e:
            raise TablespaceMigrationError( \
                u"Unable to create record for %s. Perhaps it already exists? Exception was: %s" % \
                    (form_data,str(e)))

        #
        # POST-SAVE FIELDS
        # 
        for instance_key,form_key in self.postsave_field_map.iteritems():
            logging.info("postsave_field: %s:%s" % (instance_key,form_key))

            try:
                convertor,conversion_value = self.process_field(instance_key,form_key,raw_object)
                instance_value = convertor.convert(conversion_value,form_data,None)
                setattr(instance,instance_key,instance_value)
            except IndexError:
                logging.info( \
                    "Unable to index key=%s in tablespace=%s. Skipping." % \
                        (convertor.field_name,self.tablespace))
            except KeyError:
                logging.info( \
                    "Unable to index key=%s in form in tablespace=%s. Skipping." % \
                        (form_key,self.tablespace))
            except AttributeError:
                logging.info( \
                    "Unable to set attribute=%s on instance=%s in tablespace=%s. Skipping." % \
                        (instance_key,instance,self.tablespace))

        instance.save()

        #
        # POST-SAVE RELATIONS
        #
        for key, relations in self.postsave_relation_map.iteritems():
            logging.info("postsave_relation: %s:%s" % (key,relations))
            self.process_relation(key,relations,raw_object,instance=instance)

        return instance

    def handle(self, limit=0):
        """ """
        if not self.update:
            for dependent_model_cls in self.dependent_models:
                logging.warning( "Attribute `update_existing` is not set. Deleting all objects given by '%s'" % (dependent_model_cls.query))
                if type(dependent_model_cls) == QuerySet: dependent_model_cls.delete()
                else:                                     dependent_model_cls.objects.all().delete()
            if self.queryset:
                logging.warning( "Attribute `update_existing` is not set. Deleting all objects given by '%s'" % (self.queryset.all().query))
                self.queryset.all().delete()
            else:
                logging.warning( "Attribute `update_existing` is not set. Deleting all %s objects" % (self._meta.model))
                self.model_cls.objects.all().delete()

        print "%s.migration_object with tablespace=%s" % (self.__class__.__name__,self.tablespace)

        cnt = 1

        for record in self.db.get_objects(self.tablespace,self.conditions,self.additional_tablespaces):
            self.migrate_object(record)
            cnt += 1
            if limit and cnt > limit:
                break

class TablespaceMigrationNotRegistered(Exception):
    pass

class TablespaceMigrationRegistry(object):
    """ """
    registry = {}

    # @classmethod
    # def get_migrations(cls, tablespace): # , migration_cls=None):
    #     if tablespace in cls.registry.keys():
    #         for migration_tuple in cls.registry[tablespace]:
    #             yield migration_tuple

    @classmethod
    def get_migration(cls, migration_name): #, tablespace):
        try:
            return cls.registry[migration_name]
        except KeyError:
            raise TablespaceMigrationNotRegistered( \
                u"Migration given by %s has not been registered" % (migration_name))
            
    @classmethod
    def _register(cls, migration_cls):
        migration_mod = migration_cls.__module__.split('.')[0]
        migration_name = "%s.%s" % (migration_mod.lower(),migration_cls.__name__.lower())
        cls.registry[migration_name] = migration_cls

    @classmethod
    def register_migration(cls, migration_cls):
        # tablespace = migration_cls._meta.tablespace
        # cls._register(tablespace,migration_cls)
        cls._register(migration_cls)
