from collections import MutableSequence
from pymongo import MongoClient
import datetime

client = MongoClient()
db = client.test_database

def settings(**kwargs):
    """ Decorater to populate the `_settings` attribute of a definition class
    """
    def setit(cls):
        if kwargs:
            setattr(cls, '_settings', {})
        for key, val in kwargs.items():
            cls._settings[key] = val
        return cls
    return setit

class MutableArray(MutableSequence):
    """ Strongly typed container which acts like a list
    All the regular list operations are supported """

    def __init__(self, item_setter, data=None, ):
        self.item_setter = item_setter
        self._list = list()

        if not data is None:
            if hasattr(data, '__iter__'):
                self.extend(data)
            else:
                raise TypeError("%s is not an iterable sequence" % repr(data))

    def __setitem__(self, index, value):
        self._list[index] = self.item_setter(value)

    def __getitem__(self, index):
        return self._list[index]

    def __delitem__(self, index):
        del self._list[index]

    def __len__(self):
        return len(self._list)

    def __str__(self):
        return str(self._list)

    def insert(self, index, value):
        self._list.insert(index, self.item_setter(value))

class Field(object):
    def __call__(self, value):
        raise NotImplemented("Override this method")

class Double(Field):
    def __call__(self, value):
        return value

class String(Field):
    def __call__(self, value):
        return value

class Array(Field):
    def __init__(self, contains):
        # @TODO Check for base class Field
        if type(contains) not in (String, Integer):
            raise TypeError("Please choose a suitable field type")
        self.item_setter = contains

    def __call__(self, values):
        arr =  MutableArray(self.item_setter, values)
        return arr

class Binary(Field):
    pass

class ObjectId(Field):
    pass

class Boolean(Field):
    def __call__(self, value):
        if value not in (True, False):
            raise TypeError("Not a valid boolean")
        return value

class Date(Field):
    def __call__(self, value):
        if not isinstance(value, datetime.datetime):
            raise TypeError("Expected a datetime")

class Integer(Field):
    def __call__(self, value):
        if not isinstance(value, int):
            raise TypeError("That's not an integer!")
        return value



class SettingsBase(object):
    """ Settings container
    @TODO Not sure where to put default settings yet """

    def __init__(self, *args, **kwargs):
        attrs = [a for a in self.__class__.__dict__
                            if not a.startswith('_')]
        for key in set(kwargs.keys() + attrs):
            setattr(self, "_%s" % key, kwargs[key])

    @property
    def collection(self):
        return self._collection

class DocBase(type):
    """ Metaclass for Base """

    def __new__(cls, name, bases, attrs):
        new_class = type.__new__
        if name == 'Base':
            return new_class(cls, name, bases, attrs)

        new_attrs = {}

        new_attrs['_meta'] = {}
        for obj_name, obj in attrs.items():
            if obj_name.startswith('__'):
                continue
            if obj_name == 'Settings':
                new_attrs['_settings'] = SettingsBase(**obj.__dict__)
            else:
                new_attrs['_meta'][obj_name] = obj

        return new_class(cls, name, bases, new_attrs)



class Base(object):
    """ Base class for the models that represent a scheme

    """

    __metaclass__ = DocBase

    def __init__(self, *args, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

    def __setattr__(self, name, value):
        if name in self._meta.keys():
            object.__setattr__(self, name, self._meta[name](value))
        elif getattr(self._settings, 'overload', None):
            object.__setattr__(self, name, value)
        else:
            raise AttributeError("`%s` is not defined on %s, not one of: `%s`" %
                                (name, repr(self.__class__.__name__),
                                    ', '.join(self._meta.keys())))

    def insert(self):
        doc = self.serialize()
        collection = getattr(self._settings, 'collection')\
                        or self.__class__.__name__.lower()
        db[collection].insert(doc)

    def serialize(self):
        serialized = {}
        for key, value in self.__dict__.items():
            if type(value) is MutableArray:
                value = list(value)
            serialized[key] = value

        return serialized

if __name__ == '__main__':

    class Aap(Base):
        title = String()
        keywords = Array(Integer())

        class Settings:
            collection = 'aapjes'


    a = Aap()
    a.title = 'boe'
    a.keywords = [1,2,3]
    a.keywords.append(121)
    a.insert()
