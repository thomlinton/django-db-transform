from dateutil import parser
import logging


class TablespaceValueConversionError(Exception):
    pass

class TablespaceValueConversion(object):
    field_name = ''

    def __init__(self, field_name=''):
        super(TablespaceValueConversion,self).__init__()
        if not self.field_name and field_name:
            self.field_name = field_name
        if not self.field_name and self.field_name is not None:
            raise TablespaceValueConversionError( \
                u"TablespaceValueConversion.field_name must be defined (in %s)" % (self.__class__.__name__))

    def convert(self, raw_value, raw_object, form_data):
        raise NotImplementedError

class SimpleConversion(TablespaceValueConversion):
    """
    Naive ``Conversion`` object to directly index ``raw_object`` with the given value.

    """
    def convert(self, raw_value, raw_object, form_data):
        return raw_object[self.field_name]

class ConcatinationConversion(TablespaceValueConversion):
    """
    Convenience ``Conversion`` object to support shorthand "tuple" notation.

    """
    field_name = None

    def convert(self, raw_value, raw_object, form_data):
        print "raw_value=%s raw_object=%s form_data=%s" % (raw_value,raw_object,form_data)
        return ' '.join([ raw_object[field_name] for field_name in raw_value ])

class DynamicSourceConversion(TablespaceValueConversion):
    """
    Allows a field to adapt to multiple source fields depending on the local context
    (what's in the data dictionary passed to it).

    """
    field_name = None

    def get_source_field(self, raw_value, raw_object, form_data):
        raise NotImplementedError

    def convert(self, raw_value, raw_object, form_data):
        source_field = self.get_source_field(raw_value,raw_object,form_data)
        return raw_object[ source_field ]

class BooleanConversion(TablespaceValueConversion):
    """
    Provides a convenient means of converting an enumerable set of values of arbitrary type
    to the Boolean domain (0,1), {True,False}, &c.

    """
    truth_mapping = {}

    def convert(self, raw_value, raw_object, form_data):
        truth_map_normalized = False
        truth_map = self.truth_mapping.copy()

        if all([ (type(key) == str or type(key) == unicode) for key in truth_map.keys()]):
            truth_map = dict([ (key.lower(),val) for key,val in truth_map.iteritems() ])
            truth_map_normalized = True

        if truth_map_normalized:
            raw_value = raw_value.lower()
        if raw_value not in truth_map.keys():
            return False

        return truth_map[raw_value]

class CleanConversion(TablespaceValueConversion):
    strip_chars = ''

    def clean_func(self, raw_value ):
        return raw_value

    def convert(self, raw_value, raw_object, form_data):
        return self.clean_func( \
            raw_value.strip( self.strip_chars )
            )

class FlagConversion(TablespaceValueConversion):
    def condition_func(self, raw_value, raw_object):
        raise NotImplementedError

    def convert(self, raw_value, raw_object, form_data):
        return self.condition_func(raw_value,raw_object)

class DateToDateTimeConversion(TablespaceValueConversion):
    """
    Upgrades a date object given as input to a naive ``datetime.datetime`` object.

    """
    def convert(self, raw_value, raw_object, form_data):
        if raw_value:
            return parser.parse("%s 00:00"%(raw_value))
        return u""

class DateOrNoneConversion(TablespaceValueConversion):
    """
    Attempts to convert a date object; upon failure returns None in order to circumvent
    form validation.

    """
    def convert(self, raw_value, raw_objects, form_data):
        translated_value = None
        if raw_value:
            try:
                translated_value = parser.parse(raw_value)
            except ValueError:
                pass
        return translated_value

class ChoiceConversion(TablespaceValueConversion):
    """
    A customizable conversion suited to mapping sets of values or ranges of values 
    according to rules defined on a case by case basis.

    ``normalize`` is a synonym for, e.g., ILIKE
    ``substring_check`` will fall back to a method of partial (substring) matches.
    ``shadow_field`` allows for the specification of a field to hold raw values for unsuccessful mapping/translations.
    ``strip_chars`` allows for the specification of characters to strip from raw value during normalization.

    """
    normalize = True
    substring_check = False
    default_value = None
    shadow_field = None
    choices = tuple()
    strip_chars = ''
    callback = None

    def normalize_choices(self, choices=None):
        if not choices:
            choices = self.choices
        choice_mapping = dict([ (u"%s"%choice[1].lower().strip(self.strip_chars),choice[0]) for choice in choices ])
        if not self.normalize:
            choice_mapping = dict([ (u"%s"%choice[1],choice[0]) for choice in choices ])        
        return choice_mapping

    def translate_value(self, translation_key, choice_mapping):
        mapped_value = self.default_value
        if translation_key in choice_mapping.keys(): # value -> key 
            mapped_value = choice_mapping[translation_key]

        if self.substring_check:
            for key in choice_mapping.keys():
                if key.find( translation_key ) >= 0:
                    print "Found translation_key=%s in key=%s" % (translation_key,key)
                    mapped_value = choice_mapping[key]
                    break

        return mapped_value

    def map_value(self, raw_value, raw_object, choice_mapping, form_data):
        normalized_value = raw_value
        if self.normalize:
            normalized_value = raw_value.lower().strip(self.strip_chars)

        mapped_value = self.translate_value( normalized_value, choice_mapping )
        logging.info( "Converted %s -> %s" % (raw_value,mapped_value) )
        if not mapped_value and self.shadow_field:
            logging.info( \
                "[shadow_field enabled] Placing raw_value=%s under key=%s" % (raw_value,self.shadow_field))
            raw_object[self.shadow_field] = raw_value
        if self.callback:
            logging.info( \
                "[field (mapping) callback enabled] Executing callback for field=%s" % (self.field_name))
            self.callback( raw_value, raw_object, choice_mapping, mapped_value, form_data )

        return mapped_value

    def convert(self, raw_value, raw_object, form_data):
        return self.map_value(
            raw_value, raw_object, self.normalize_choices(), form_data
            )

class MultipleChoiceConversion(ChoiceConversion):
    separator = ','

    def map_value(self, raw_values, raw_object, choice_mapping, form_data):
        mapped_values = []
        for raw_value in raw_values.split(self.separator):
            mapped_value = \
                super(MultipleChoiceConversion,self).map_value(raw_value,raw_object,choice_mapping,form_data)
            if mapped_value:
                mapped_values.append( mapped_value )

        return mapped_values

class MultipleColumnChoiceConversion(ChoiceConversion):
    field_names = []
    normalize = False

    def map_value(self, raw_value, raw_object, choice_mapping, form_data):
        mapped_values = []
        for field_name in self.field_names:
            mapped_value = \
                super(MultipleColumnChoiceConversion,self).map_value( \
                    raw_object[field_name], raw_object, choice_mapping, form_data
                    )
            if mapped_value:
                mapped_values.append( mapped_value )

        return mapped_values
