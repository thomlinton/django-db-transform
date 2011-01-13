from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ObjectDoesNotExist
from django.forms.models import modelform_factory

import logging


class TablespaceRelationBindingError(Exception):
    pass

class TablespaceRelationBindingOptions(object):
    """ """
    migration = None
    update = False
    fetch = True

    #
    # TODO: Figure out a more elegant way of implementing
    #
    key_type = long
    primary_key = ''
    local_key = ''
    remote_key = ''

    content_type_field_name = 'content_type'
    object_id_field_name = 'object_id'
    related_field_name = 'parent'

    def __init__(self, opts):
        if opts:
            for key,value in opts.__dict__.iteritems():
                setattr(self, key, value)

class TablespaceRelationBindingBase(type):
    """ """
    def __new__(cls, name, bases, attrs):
        super_new = super(TablespaceRelationBindingBase, cls).__new__
        parents = [b for b in bases if isinstance(b, TablespaceRelationBindingBase)]
        if not parents:
            # If this isn't a subclass of TablespaceRelationBinding, don't do anything special.
            return super_new(cls, name, bases, attrs)

        module = attrs.pop('__module__')
        # new_class = super_new(cls, name, bases, {'__module__': module})
        new_class = super_new(cls, name, bases, attrs)
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta

        meta = TablespaceRelationBindingOptions(meta)

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
                        raise TablespaceRelationBindingError( \
                            u"Attribute %s is unset: %s" % (key,str(e)))

        setattr(new_class, '_meta', meta)
        return new_class

class TablespaceRelationBinding(object):
    """ """
    __metaclass__ = TablespaceRelationBindingBase

    def __init__(self, parent_migration):
        super(TablespaceRelationBinding,self).__init__()

        self.parent_migration = parent_migration
        if not self._meta.migration:
            raise TablespaceRelationBindingError( \
                u"The migration attribute must be defined")
        tablespace = None
        if not self._meta.migration._meta.tablespace:
            tablespace = self.parent_migration.tablespace
        self.migration = self._meta.migration(tablespace=tablespace)
        self.update = self._meta.update
        self.fetch = self._meta.fetch

    def get_lookup_attributes(self, raw_object, instance):
        """
        Hook to populate an (related) object lookup dictionary.

        """
        return {}

    def get_raw_lookup_attributes(self, raw_object, instance):
        """
        Hook to populate an (related) raw object lookup dictionary.

        """
        return {}

    def add_to_form(self, form_data, form_key, instance):
        """
        Allows the implementation of a ``TablespaceRelationBinding`` object
        to define the semantics of binding itself to a form.

        """
        pass

    def add_to_lookup(self, lookup, key, instance):
        """
        Allows the implementation of a ``TablespaceRelationBinding`` object
        to define the semantics of binding itself to a lookup dictionary.

        """
        pass

    def handle(self, raw_object, parent):
        """ """
        raw_lookup = self.get_raw_lookup_attributes(raw_object, parent)
        lookup = self.get_lookup_attributes(raw_object, parent)
        related_raw_object = raw_object
        if self.fetch:
            related_raw_object = self.migration.get_raw_object(raw_lookup)
        related_obj = self.migration.get_object( \
            raw_object, extra_lookup=lookup)

        print "related_obj=%s" % (related_obj)
        print "raw_lookup=%s" % (raw_lookup)
        print "lookup=%s" % (lookup)
        print "update=%s" % (self.update)

        if related_obj and not self.update:
            return related_obj
        return self.migration.migrate_object(related_raw_object,related_obj)

class ForeignKeyBinding(TablespaceRelationBinding):
    """ 
    Represents a ForeignKey relation.
    
    Establishes a link between data created in this tablespace to another
    object of arbitrary type (the current object context).
    
    """
    class Meta:
        key_type = long # key_type is an integer by default
        primary_key = '' # data model primary key attribute name
        local_key = '' # tablespace (local) attribute name
        remote_key = '' # tablespace (remote) attribute name (used to search distinct tablespace, if necessary)

    def __init__(self, parent_migration):
        super(ForeignKeyBinding,self).__init__(parent_migration)

        self.key_type = self._meta.key_type
        if not self._meta.primary_key:
            raise TablespaceRelationBindingError( \
                u"The primary_key attribute must be defined")
        self.primary_key = self._meta.primary_key
        self.local_key = self._meta.local_key
        if not self.local_key:
            self.local_key = self.primary_key
        self.remote_key = self._meta.remote_key
        if not self.remote_key:
            remote_reference_key = local_reference_key

    def get_lookup_attributes(self, raw_object, instance):
        """ """
        try:
            return {self.primary_key:self.key_type(raw_object[self.local_key])}
        except (IndexError,TypeError,ValueError):
            logging.warning( \
                u"Source data %s did not define local_key=%s or key_type=%s could not transform an invalid value" % \
                    (dict(raw_object),self.local_key,self.key_type))
            return {}

    def get_raw_lookup_attributes(self, raw_object, instance):
        """ """
        try:
            return {self.remote_key:self.key_type(raw_object[self.local_key])}
        except (IndexError,TypeError,ValueError):
            logging.warning( \
                u"Source data %s did not define local_key=%s or key_type=%s could not transform an invalid value" % \
                    (dict(raw_object),self.local_key,self.key_type))
            return {}

    def add_to_form(self, form_data, form_key, instance):
        form_data[form_key] = instance.pk

    def add_to_lookup(self, lookup, key, instance):
        lookup[key] = instance

#
# TODO: Complete implementation of ``GenericForeignKey``
#
class GenericForeignKeyBinding(ForeignKeyBinding):
    """
    Represents a GenericForeignKey relation.

    """
    class Meta:
        content_type_field_name = 'content_type'
        object_id_field_name = 'object_id'

    def __init__(self, parent_migration):
        super(GenericForeignKeyBinding,self).__init__(parent_migration)
        self.content_type_field_name = self._meta.content_type_field_name
        self.object_id_field_name = self._meta.object_id_field_name

    # def add_to_form(self, form_data, form_key, instance):
    #     pass

    # def add_to_lookup(self, lookup, key, instance):
    #     pass

    def handle(self, raw_object, parent):
        raise NotImplementedError

class ManyToManyBinding(ForeignKeyBinding):
    """ 
    Represents a ManyToManyField relation.
    
    """
    def add_to_form(self, form_data, form_key, instance):
        print "ManyToManyBinding.add_to_form: "
        if form_key not in form_data or not form_data[form_key] or type(form_data[form_key]) != list:
            form_data[form_key] = list()
        form_data[form_key].append( instance.pk )

    def add_to_lookup(self, lookup, key, instance):
        if key not in lookup or not lookup[key] or type(lookup[key]) != list:
            lookup[key] = list()
        lookup[key].append( instance )

class RelationBinding(TablespaceRelationBinding):
    """
    Represents a 'reverse' ForeignKey relation.

    """
    class Meta:
        related_field_name = 'parent'
        fetch = False

    def __init__(self, parent_migration):
        super(RelationBinding,self).__init__(parent_migration)
        self.related_field_name = self._meta.related_field_name

    def get_lookup_attributes(self, raw_object, instance):
        return {'%s'%(self.related_field_name):instance}

    def get_raw_lookup_attributes(self, raw_object, instance):
        raise NotImplementedError

    def get_bind_params(self, raw_object, instance):
        return {'%s'%(self.related_field_name):instance.pk}

    def add_to_form(self, form_data, form_key, instance):
        return

    def add_to_lookup(self, lookup, key, instance):
        raise NotImplementedError

    #
    # TODO: Determine what is best to do here
    #
    def handle(self, raw_object, parent):
        lookup = self.get_lookup_attributes(raw_object, parent)
        related_obj = self.migration.get_object( \
            raw_object, extra_lookup=lookup)

        # if related_obj:
        #     return related_obj
        # related_obj = self.get_bind_params(parent)

        bind_params = self.get_bind_params(raw_object,parent)
        return self.migration.migrate_object(raw_object,instance=related_obj,initial=bind_params)

class GenericRelationBinding(RelationBinding):
    """
    Represents a GenericForeignKey relation.
    
    """
    class Meta:
        content_type_field_name = 'content_type'
        object_id_field_name = 'object_id'

    def __init__(self, parent_migration):
        super(GenericRelationBinding,self).__init__(parent_migration)
        self.content_type_field_name = self._meta.content_type_field_name
        self.object_id_field_name = self._meta.object_id_field_name

    def get_lookup_attributes(self, raw_object, instance):
        return {
            '%s'%(self.content_type_field_name): ContentType.objects.get_for_model(instance),
            '%s'%(self.object_id_field_name): instance.pk
            }

    def get_bind_params(self, raw_object, instance):
        return {
            '%s'%(self.content_type_field_name): ContentType.objects.get_for_model(instance).pk,
            '%s'%(self.object_id_field_name): instance.pk
            }
