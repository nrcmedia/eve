from collections import MutableSequence
from pymongo import MongoClient
import datetime
from bson import objectid
from bson.errors import InvalidId

client = MongoClient()
db = client.test_database

class ValidationError(Exception):
    pass

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
    """ Strongly typed container class which acts like a list
    all the usual list operations are supported """

    def __init__(self, item_setter, data=None, ):
        self.item_setter = item_setter
        self._list = list()

        if not data is None:
            if hasattr(data, '__iter__'):
                self.extend(data)
            else:
                raise TypeError("`%s` is not an iterable sequence" % repr(data))

    def set(self, value):
        if callable(self.item_setter):
            prop = self.item_setter(value)
        else:
            prop = self.item_setter
            for key, val in value.iteritems():
                setattr(prop, key, val)
        return prop

    def __setitem__(self, index, value):
        self._list[index] = self.set(value)

    def __getitem__(self, index):
        return self._list[index]

    def __delitem__(self, index):
        del self._list[index]

    def __len__(self):
        return len(self._list)

    def __str__(self):
        return str(self._list)

    def insert(self, index, value):
        print value
        self._list.insert(index, self.set(value))

class Field(object):
    def __init__(self, required=False):
        self.required = required

    def __call__(self, value):
        raise NotImplemented("Override this method")

class Double(Field):
    def __call__(self, value):
        return value

class String(Field):
    def __call__(self, value):
        if not isinstance(value, basestring):
            raise TypeError("`%s` is not an instance of basestring" % repr(value))
        return value

class Array(Field):
    def __init__(self, contains):
        if not isinstance(contains, (Field, Base)):
            raise TypeError("Please choose a suitable field type for this container")
        self.item_setter = contains

    def __call__(self, values):
        arr =  MutableArray(self.item_setter, values)
        return arr

class Binary(Field):
    pass


class ObjectId(Field):
    def __call__(self, value):
        try:
            value = objectid.ObjectId(value)
        except (InvalidId, TypeError) as e:
            raise TypeError(e)

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

    @classmethod
    def setter(cls, prop):
        if isinstance(prop, Field):
            return prop
        elif isinstance(prop, Base):
            return prop

    def __setattr__(self, name, value):
        if name in self._meta.keys():
            prop = self._meta[name]
            if isinstance(prop, Field):
                return object.__setattr__(self, name, self._meta[name](value))
            elif isinstance(prop, Base):
                for key, val in value.iteritems():
                    setattr(prop, key, val)
                return object.__setattr__(self, name, prop)

        elif getattr(self._settings, 'overload', None):
            return object.__setattr__(self, name, value)
        else:
            raise AttributeError("`%s` is not defined on %s, not one of: `%s`" %
                                (name, repr(self.__class__.__name__),
                                    ', '.join(self._meta.keys())))

    @classmethod
    def validate(cls, prop, value):
        try:
            validator = cls._meta[prop]
        except KeyError:
            raise ValidationError('Unknown property `%s` on class `%s`' % (prop, cls.__name__))
        try:
            val = validator(value)
        except TypeError as e:
            raise ValidationError(e)
        else:
            return val

    def __str__(self):
        return str(self.__dict__)

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

    class Nested(Base):
        name = String()

    class Block(Base):
        type = String()
        content = String()
        lijst = Array(Nested())

    class Aap(Base):
        test = Block()

        producer = String()
        external_id = String()
        title = String()
        headline = String()
        abstract = String()
        kicker = String()
        authors = Array(ObjectId())
        images = Array(ObjectId())
        publication = ObjectId()
        keywords = Array(String())
        genres = Array(String())
        blocks = Array(Block())
        page_start = Integer()
        page_end = Integer()
        wordcount = Integer()
        filename = String()
        content = String()
        # @TODO urn type would be nice!
        urn = String()
        published_at = Date()
        slug = String()

    aapje = Aap()
    aapje.title = 'testing'
    aapje.blocks = [{'content': 'jatoch', 'lijst': [{'name': 'janeetoch'}]}]
    print aapje

    #Aap.validate('blocks', [1,2])
    #Aap.validate('genres', ['1', '2', '3'])
    #Aap.validate('authors', ['52ef8208bfdf2a6712b7fc06'])
    #Aap.validate('ja toch', 1)

        #a.title = 'boe'
    #a.keywords = [1,2,3]
    #a.keywords.append(121)
    #a.insert()

