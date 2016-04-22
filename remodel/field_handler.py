from inflection import tableize

from .errors import AlreadyRegisteredError
import remodel.models
from .registry import index_registry
from .related import (HasOneDescriptor, BelongsToDescriptor, HasManyDescriptor,
                     HasAndBelongsToManyDescriptor)


class FieldHandlerBase(type):
    def __new__(cls, name, bases, dct):
        if not all(isinstance(dct[rel_type], tuple) for rel_type in remodel.models.REL_TYPES):
            raise ValueError('Related models must be passed as a tuple')

        # TODO: Find a way to pass model class to its field handler class
        model = dct.pop('model')
        dct['restricted'], dct['related'] = set(), set()
        for rel in dct.pop('has_one'):
            if isinstance(rel, tuple):
                # 4-tuple relation supplied
                other, field, lkey, rkey = rel
            else:
                # Just the related model supplied
                other = rel
                field, lkey, rkey = other.lower(), 'id', '%s_id' % model.lower()
            dct[field] = HasOneDescriptor(other, lkey, rkey)
            dct['related'].add(field)
            index_registry.register(other, rkey)
        for rel in dct.pop('belongs_to'):
            if isinstance(rel, tuple):
                other, field, lkey, rkey = rel
            else:
                other = rel
                field, lkey, rkey = other.lower(), '%s_id' % other.lower(), 'id'
            dct[field] = BelongsToDescriptor(other, lkey, rkey)
            dct['related'].add(field)
            dct['restricted'].add(lkey)
            index_registry.register(model, lkey)
        for rel in dct.pop('has_many'):
            if isinstance(rel, tuple):
                other, field, lkey, rkey = rel
            else:
                other = rel
                field, lkey, rkey = tableize(other), 'id', '%s_id' % model.lower()
            dct[field] = HasManyDescriptor(other, lkey, rkey)
            dct['related'].add(field)
            index_registry.register(other, rkey)
        for rel in dct.pop('has_and_belongs_to_many'):
            if isinstance(rel, tuple):
                other, field, lkey, rkey = rel
            else:
                other = rel
                field, lkey, rkey = tableize(other), 'id', 'id'
            join_model = '_' + ''.join(sorted([model, other]))
            try:
                remodel.models.ModelBase(join_model, (remodel.models.Model,), {})
            except AlreadyRegisteredError:
                # HABTM join_model model has been registered, probably from the
                # other end of the relation
                pass
            mlkey, mrkey = '%s_id' % model.lower(), '%s_id' % other.lower()
            dct[field] = HasAndBelongsToManyDescriptor(other, lkey, rkey, join_model, mlkey, mrkey)
            dct['related'].add(field)
            index_registry.register(join_model, mlkey)
            index_registry.register(join_model, mrkey)

        return super(FieldHandlerBase, cls).__new__(cls, name, bases, dct)


class FieldHandler(object):
    def __getattribute__(self, name):
        if name in super(FieldHandler, self).__getattribute__('restricted'):
            raise AttributeError('Cannot access %s: field is restricted' % name)
        return super(FieldHandler, self).__getattribute__(name)

    def __setattr__(self, name, value):
        if name in self.restricted:
            raise AttributeError('Cannot set %s: field is restricted' % name)
        super(FieldHandler, self).__setattr__(name, value)

    def __delattr__(self, name):
        if name in self.restricted:
            raise AttributeError('Cannot delete %s: field is restricted' % name)
        super(FieldHandler, self).__delattr__(name)

    def as_dict(self):
        return {field: self.__dict__[field] for field in self.__dict__
                if not field.startswith('_')}


class Field(object):
    ''' Base class for fields
    It doesn't handle values directly but maps the instance.fields handler
    by the field name for getting and setting values. Conversions of complex
    objects would occur during a get or set operation through
    modelinstance.fieldname calling internally to_native and to_rdb respectively '''
    def __init__(self, **kwargs):
        self.name = None
        if 'default' in kwargs:
            self.default = kwargs['default']
        else:
            self.default = None

    def __get__(self, instance, owner):
        return getattr(instance.fields, self.to_native(self.name))

    def __set__(self, instance, value):
        if self.validate(value):
            instance[self.name] = self.to_rdb(value)

    def __delete__(self, instance, value):
        delattr(instance, self.name)

    def set_name(self, name):
        self.name = name

    def validate(self, value):
        # Just check if we have something
        if value is None:
            if self.name:
                raise ValueError('{0} type is not a valid type'.
                                 format(self.name))
            else:
                raise ValueError('{0} type is not a valid type'.format(self))
        return True

    def to_native(self, value):
        # convert to python data type if necessary
        return value

    def to_rdb(self, value):
        # convert to rethinkdb compatible data type if necessary
        return value


class NumericField(Field):
    # We don't need a custom adaptation between a python numeric type (int, float)
    # and so, we just validate
    def validate(self, value):
        if super(NumericField, self).validate(value) and (isinstance(value, int) or isinstance(value, float)):
            return True
        else:
            if self.name != None:
                raise ValueError('{0} is not a numeric type: {1}'.format(self.name, value))
            else:
                raise ValueError('{0} is not a numeric type'.format(value))


class StringField(Field):
    def validate(self, value):
        if not isinstance(value, str):
            if self.name != None:
                raise ValueError('{0} is not a string type: {1}'.format(self.name, value))
            else:
                raise ValueError('"{0}" is not a string type'.format(value))
        return True
