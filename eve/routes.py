# -*- coding: utf-8 -*-

"""
    eve.methods.common
    ~~~~~~~~~~~~~~~~~~

    Utility functions for API methods implementations.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

import time
from werkzeug.utils import escape
from datetime import datetime
from collections import namedtuple
from flask import current_app as app, abort as flask_abort, request, g, Response, make_response, url_for, Response
from flask.views import MethodView
from eve.utils import parse_request, document_etag, config, \
    request_method, debug_error_message
from bson.objectid import ObjectId
from functools import wraps
from eve.io.mongo import Validator, Mongo
from eve.helpers import str_to_date, jsonify, document_link
from bson.errors import InvalidId
from bson.son import SON
from bson import json_util
from dateutil.tz import tzlocal
from eve.signals import pre_insert, pre_update, pre_fetch
from eve.errors import abort
import logging
import re
import json
from pymongo.errors import PyMongoError
from pprint import pprint
from collections import defaultdict

COMPARISON = [
    "$gt",
    "$gte",
    "$in",
    "$lt",
    "$lte",
    "$ne",
    "$nin",
]

ALLOWED = [
    "producer",
]

logger = logging.getLogger('mongrest')

class RequestContext(object):
    """ The request context
    @see get_context """

    def __init__(self, limit, offset, query, embedded, projection, sort, aggregate):
        self.limit = limit
        self.offset = offset
        self.query = query
        self.embedded = embedded
        self.projection = projection
        self.sort = sort
        self.aggregate = aggregate

class ApiView(MethodView):
    """ Pluggable view for RESTful crud operations on mongo collections,
    All the HTTP methods go here """

    pk = '_id'

    def __init__(self, driver, resource, *args, **kwargs):
        """ Calls the init of the superclass and specifies the resource to which
        this view provides the API endpoints

        @TODO Mongo operations could use a wrapper
        @TODO http://flask.pocoo.org/docs/patterns/apierrors/
        """

        super(ApiView, self).__init__(*args, **kwargs)
        self.db = driver.db
        self.resource = resource
        self.collection = driver.db[resource]
        self.resource_def = app.config['DOMAIN'][resource]
        self.schema = app.config['DOMAIN'][resource]['schema']
        self.reference_keys = _find_objectid_fields(self.schema)
        self.date_keys = _find_date_fields(self.schema)
        self.validator = Validator(self.schema, self.db, resource)

        # Subscribe to signals
        pre_insert.connect(ApiView.pre_insert)
        pre_update.connect(ApiView.pre_update)

    @staticmethod
    def _pre_insert_update(sender, **kwargs):
        """ Gets after the parsing the payload in POST/PUT requests,
        add computed fields here """
        doc = kwargs.get('doc')
        if doc:
            _add_timers(doc)


    @staticmethod
    def pre_insert(sender, **kwargs):
        """ Gets called just before inserting the document in the database,
        useful for adding timestamps """

        ApiView._pre_insert_update(sender, **kwargs)


    @staticmethod
    def pre_update(sender, **kwargs):
        """ Called after PUT payload is parsed """
        ApiView._pre_insert_update(sender, **kwargs)


    @staticmethod
    def after_get(sender, **kwargs):
        """ """
        ApiView._pre_insert_update(sender, **kwargs)

    def _parse_validate_payload(self, parse_embedded=True, patchmode=False):
        """ Parse and validate payload from the request, optinionally handle additional
        embedded resources. Used by POST and PUT requests """

        if not request.is_json:
            abort(400, 'Not a valid Content-Type')

        payload = request.get_json()
        doc = self._parse(payload)

        # Pluck the '_embedded' key from the object before passing the primary doc
        # to the validator, we want to validate the embedded resources seperately
        embedded = doc.pop('_embedded', None)

        # Validation, abort request on any error
        if not patchmode: # @TODO Patchmode entirely skips validation now
            validated = self.validator.validate(doc)
            if not validated:
                abort(400, self.validator.errors)

        if parse_embedded and hasattr(embedded, 'items'):
            self._parse_embeds(payload, embedded, doc)

        return doc

    def _parse_embeds(self, payload, embedded, doc):
        """ POST, PUT and PATCH requests accept embedded resource """

        for key, embed in embedded.items():
            if key in payload:
                # Embedded doc conflicts with the same key in the root doc
                abort(400, '{%s} present as embedded field and reference')

            ref = self.reference_keys[key]
            if ref['multi']:
                if not type(embed) is list:
                    abort(400, '{%s} requires list of values' % key)
                for item in embed:
                    _id = get_or_create(self.db[ref['resource']], self.db, ref['resource'], item)
                    if _id:
                        # Set the reference list
                        doc.setdefault(key, [])
                        doc[key].append(_id)
                    else:
                        # Array order may have changed
                        pass

            else:
                _id = get_or_create(self.db[ref['resource']], self.db, ref['resource'], embed)
                if _id:
                    doc[key] = _id

    def get(self, context=None, **kwargs):
        """ GET requests """
        params = context or get_context()

        if all(v is None for v in kwargs.values()):
            # List endpoint when no keyword args
            find_args = []
            if params.query:
                find_args.append(params.query)
            if params.projection:
                if not len(find_args):
                    find_args.append({})
                find_args.append(params.projection)

            if params.aggregate:
                aggr = params.aggregate
                aggr_pipes = {}
                for i, part in enumerate(aggr):
                    aggr_pipes[part.keys()[0]] = i
                # pymongo expects SON object for $sort param
                if '$sort' in aggr_pipes.keys():
                    aggr[aggr_pipes['$sort']] = {'$sort': SON([tuple(p) for p in aggr[aggr_pipes['$sort']]['$sort']])}
                # Enforce upper limit when no limit is set in the aggregration
                if '$limit' not in aggr_pipes.keys():
                    aggr.append({'$limit': 25}) # @TODO should inherit from settings

                cursor = self.collection.aggregate(aggr, cursor={})
            else:
                cursor = self.collection.find(*find_args)\
                            .skip(params.offset).limit(params.limit)
            if params.sort:
                cursor.sort(params.sort.items())

            documents = list(cursor)
            # Embedded documents
            if params.embedded:
                _resolve_embedded_documents(self.resource, self.db, params.embedded, documents)

            for doc in documents:
                _finalize_doc(doc, self.reference_keys, self.resource, etag=False)

            # Add paginated links
            links = _pagination_links(self.resource)

            return jsonify({'_items': documents, '_links': links}), 200

        else:
            # Item endpoint
            if not '_id' in kwargs:
                abort(400, 'only {_id} is supported')
            try:
                _id = ObjectId(kwargs['_id'])
            except (InvalidId, TypeError):
                abort(400, 'Invalid id')

            none_match = request.headers.get('If-None-Match')
            doc = self.collection.find_one({'_id': _id})
            if not doc:
                abort(404)

            # Embedded documents
            if params.embedded:
                _resolve_embedded_documents(self.resource, self.db, params.embedded, [doc])
            _finalize_doc(doc, self.reference_keys, self.resource)
            if none_match and none_match.replace('"','') == doc.get('etag'):
                abort(304)

            resp = jsonify(doc)
            if doc.get('updated_at'):
                resp.headers.set('Last-Modified',
                    doc['updated_at'].strftime("%a, %d %b %Y %H:%M:%S %Z"))

            if doc.get('etag'):
                resp.headers.set('ETag', doc['etag'])

            return resp, 200


    def post(self):
        """ POST request """

        doc = self._parse_validate_payload(parse_embedded=True)
        pre_insert.send(self, doc=doc)
        try:
            _id = self.collection.insert(doc)
        except PyMongoError as e:
            logger.error('Error executing insert: %s', e)
            abort(500, 'Something went horribly wrong T.T')
        if not _id:
            abort(500)

        # Add the id to the payload
        doc['_id'] = _id
        _finalize_doc(doc, self.reference_keys, self.resource)
        resp = jsonify(doc)
        # Add Location header
        resp.headers.set('Location', document_link(self.resource, _id))
        return resp, 201


    def put(self, **kwargs):
        """ PUT request """

        _id = kwargs["_id"]

        # @TODO patchmode totally skips any validation!
        doc = self._parse_validate_payload(parse_embedded=True, patchmode=True)
        pre_update.send(self, doc=doc)

        doc["_id"] = _id
        try:
            self.collection.save(doc)
        except PyMongoError as e:
            logger.error('Error executing insert (%s)', e)
            abort(500, 'Something went horribly wrong TT')

        _finalize_doc(doc, self.reference_keys, self.resource)

        return jsonify({}), 204


    def _parse_execute_patches(self, patches, _id):
        patches = self._parse(patches, patchmode=True)
        # Root patches ...
        if patches:
            self._patch(self.resource, _id, patches)


    def _patch(self, resource, _id, patches):
        """
            @see http://tools.ietf.org/html/draft-ietf-appsawg-json-patch-08

            { "op": "remove", "path": "/a/b/c" },
            { "op": "add", "path": "/a/b/c", "value": [ "foo", "bar" ] },
            { "op": "replace", "path": "/a/b/c", "value": 42 },

        """
        updates = defaultdict(dict)
        schema = app.config['DOMAIN'][resource]['schema']

        operations_map = {
            'remove':   {
                'single': {
                    'exists': '$unset',
                },
                'multi' : {
                    'exists': '$pull',
                },
            },
            'add': {
                'single': {
                    'exists': '$set',
                    'new': '$set',
                },
                'multi' : {
                    'exists': '$push',
                    'new': '$set',
                },
            },
            'replace': {
                'single': {
                    'exists': '$set',
                },
                'multi': {
                    'exists': '$set',
                },
            },
        }

        for operation in patches:
            prop = operation['path']
            op = operation['op']
            value = operation['value']

            # multivalued property? @TODO does not work on nested objects!
            is_multi = schema[prop.split('.')[0]]['type'] == 'list'
            exists = self.db[resource].find_one({'_id': _id, prop: {'$exists': True }})
            db_op_map = operations_map[op]['multi' if is_multi else 'single']

            db_op = db_op_map.get('exists' if exists else 'new')
            if not db_op:
                if op == 'remove':
                    abort(400, 'Cannot remove non existing field: `%s`' % op)
                elif op == 'replace':
                    abort(400, 'Cannot replace non existing field: `%s`' % op)
            else:
                updates[db_op][prop] = value

        if not updates:
            # @TODO this is a _parse error probably, doesn't belong here
            abort(400, 'PATCH requests expects list of operations, none found')

        result = self.db[resource].update({'_id': _id}, updates)

        return result.get('updatedExisting') == True


    def patch(self, **kwargs):
        """ PATCH request
        """

        if '_id' not in kwargs:
            abort(400, 'Please provide the primary key')

        if not request.is_json:
            abort(400, 'No valid Content-Type')
        payload = request.get_json()

        self._parse_execute_patches(payload, kwargs['_id'])

        # Succesful patch requests return 204 with a pointer to the
        # updated resource.
        resp = jsonify({})
        # @TODO Shouldn't this be 'Location' ?
        resp.headers.set('Content-Location', document_link(self.resource, kwargs['_id']))
        return resp, 204


    def delete(self, **kwargs):
        """ DELETE request """

        _id = kwargs["_id"]
        # @TODO Patch is idempotent, so return 204 when item does not exist
        if self.collection.remove({'_id': _id}):
            return jsonify({}), 204
        abort(400, 'Failed to delete')


    def _parse(self, payload, patchmode=False):
        """ Parse incoming request payloads """
        if type(payload) not in (dict, list):
            abort(400, "Provide a valid JSON object")
        if patchmode and type(payload) is not list:
            abort(400, "Provide an array of patches")

        if patchmode:
            for patch in payload:
                parts = patch.get('path').split('/')
                if parts.pop(0) != '':
                    abort(400, 'Patch path must start with "/"')
                parts = [part.replace('~1', '/') for part in parts]
                parts = [part.replace('~0', '~') for part in parts]
                patch['path'] = '.'.join(parts)

            return payload

        # Object id field strings to ObjectId instances
        if payload.get('_id'):
            payload['_id'] = ObjectId(payload['_id'])

        doc_references = [k for k in self.reference_keys if k in payload]
        try:
            for obj_key in doc_references:
                if self.reference_keys[obj_key].get('multi'):
                    if hasattr(payload[obj_key], '__iter__'):
                        payload[obj_key] = [ObjectId(v) for v in payload[obj_key]]
                    else:
                        payload[obj_key] = ObjectId(payload[obj_key])
                else:
                    payload[obj_key] = ObjectId(payload[obj_key])
        except (InvalidId, TypeError):
            abort(400, 'Reference fields should be 24 char hex strings')

        # Date strings to datetime objects
        doc_dates = self.date_keys.intersection(payload.keys())
        for date_key in doc_dates:
            payload[date_key] = str_to_date(payload[date_key])

        return payload

def get_or_create(collection, db, resource, payload):
    """ Helper for embededded inserts, tries to update doc from mongo when all unique fields are
    present, tries to insert a new doc when nothing is found or not all uniques are present
    When the primary key is present in the payload, try to update the doc.
    Aborts on validation errors """

    schema = app.config['DOMAIN'][resource]['schema']

    if '_id' in payload.keys():
        # Primary key is present, try to update this doc
        doc = collection.find_one({'_id': ObjectId(payload['_id'])}, {})
        if not doc:
            abort('400', {'errors': ['Could not find embedded document with id: %s' % payload['_id']]})

        # v = Validator(schema, db, resource)
        # validated = v.validate(payload)
        doc_set = payload
        del doc_set['_id']
        collection.update({'_id': doc['_id']}, {'$set': doc_set})
        return doc['_id']

    uniques = [key for key in schema if schema[key].get('unique')]
    if uniques and all(k in payload for k in uniques):
        # Unique values are present, try to find the doc
        uniq_values = dict( (k, v) for (k, v) in payload.iteritems()
                        if k in uniques)
        doc = collection.find_one(uniq_values, {})
        if doc:
            return doc['_id']

    # New instance, try and create it
    v = Validator(schema, db, resource)
    validated = v.validate(payload)
    doc = payload
    if validated:
        pre_insert.send(app._get_current_object(), doc=doc)
        _id = collection.insert(doc)
        return collection.find_one({'_id': _id}, {})['_id']

    abort('400', {'errors': v.errors})


def _finalize_doc(doc, reference_keys, resource, etag = True):
    """ Adds link and etag to the document """
    if etag:
        doc['etag'] = document_etag(doc)
    _add_links(doc, reference_keys, resource)


def _add_timers(doc):
    """ Populates computed datetime fields """

    doc['updated_at'] = datetime.now(tzlocal())
    if not doc.get('created_at'):
        doc['created_at'] = datetime.now(tzlocal())


def _find_objectid_fields(schema):
    """ Find object id fields, or reference fields in schema """

    related_keys = {}
    for key, val in schema.iteritems():
        if val['type'] == 'objectid':
            related_keys[key] = {'resource': val['data_relation']['collection'],
                'multi': False, 'schema': app.config['DOMAIN'][val['data_relation']['collection']]['schema']}

        elif val['type'] == 'list' and val['schema']['type'] == 'objectid':
            related_keys[key] = {'resource': val['schema']['data_relation']['collection'],
            'multi': True, 'schema': app.config['DOMAIN'][val['schema']['data_relation']['collection']]['schema']}

    return related_keys

def _find_date_fields(schema):
    """ Find date keys"""
    return set(field for field, definition in schema.iteritems()
            if definition.get('type') == 'datetime')


def _add_links(doc, reference_keys, resource):
    """ Find all the ObjectId references and convert them to urls """
    doc.setdefault('_links', {})
    links = {}
    links['self'] = dict(href=document_link(resource, doc.get('_id')))

    for key, rel in reference_keys.iteritems():
        if doc.get(key):
            if rel.get('multi'):
                links[key] = [dict(href=document_link(rel['resource'], r)) for r in doc[key]]
            else:
                links[key] = dict(href= document_link(rel['resource'], doc[key]))
            del doc[key]

    doc['_links'].update(links)


def _resolve_embedded_documents(resource, db, embedded, documents):
    """Loops through the documents, adding embedded representations
    of any fields that are (1) defined eligible for embedding in the
    DOMAIN and (2) requested to be embedded in the current `req`

    Currently we only support a single layer of embedding,
    i.e. /invoices/?embedded={"user":1}
    *NOT*  /invoices/?embedded={"user.friends":1}

    :param resource: the resource name.
    :param req: and instace of :class:`eve.utils.ParsedRequest`.
    :param documents: list of documents returned by the query.

    .. versionadded:: 0.1.0
    """
    # Build the list of fields where embedding is being requested
    try:
        embedded_fields = [k for k, v in embedded.items()
                           if v == 1]
    except AttributeError:
        # We got something other than a dict
        abort(400, description=debug_error_message(
            'Unable to parse `embedded` clause'
        ))

    # For each field, is the field allowed to be embedded?
    # Pick out fields that have a `data_relation` where `embeddable=True`
    enabled_embedded_fields = []
    config = app.config

    for field in embedded_fields:
        # Reject bogus field names
        if field in config['DOMAIN'][resource]['schema']:
            field_definition = config['DOMAIN'][resource]['schema'][field]
            if ('data_relation' in field_definition and \
                field_definition['data_relation'].get('embeddable')) or \
                ('schema' in field_definition and \
                'data_relation' in field_definition['schema'] and \
                field_definition['schema']['data_relation'].get('embeddable')):

                    enabled_embedded_fields.append(field)

    replace = defaultdict(list)
    _ids = {}
    for i, document in enumerate(documents):
        for field in enabled_embedded_fields:
            # Retrieve and serialize the requested document
            if not document.get(field):
                continue

            field_definition = config['DOMAIN'][resource]['schema'][field]
            if field_definition['type'] == 'list':
                collection_name = field_definition['schema']['data_relation']['collection']
            else:
                collection_name = field_definition['data_relation']['collection']

            _ids.setdefault(collection_name, list())

            if hasattr(document[field], '__iter__') and\
                hasattr(document[field], '__getitem__'):
                _ids[collection_name].extend(document[field])
                for _id in document[field]:
                    replace[str(_id)].append((i, field, field_definition['type']))
            else: # assuming single value
                _ids[collection_name].append(document[field])
                replace[str(document[field])].append((i, field, field_definition['type']))

            del document[field]


    embedded_docs = {}
    for collection_name, idlst in _ids.iteritems():
        # Mongodb docs recommend using the $in operator over the $or operator
        # @see http://docs.mongodb.org/manual/reference/operator/query/or/#_S_or%22
        results = db[collection_name].find({'_id': {'$in': list(set(idlst))}})
        embedded_docs.update({str(doc['_id']): doc for doc in results})

    if not embedded_docs:
        return

    # Attach the embedded docs to their parents
    for idstr, doc in embedded_docs.iteritems():
        toreplace = replace.get(idstr)
        if toreplace:
            for r in toreplace:
                if '_embedded' not in documents[r[0]]:
                    documents[r[0]]['_embedded'] = dict()
                if r[2] == 'list':
                    documents[r[0]]['_embedded']\
                        .setdefault(r[1], []).append(doc)
                else:
                    documents[r[0]]['_embedded'][r[1]] = doc


def _pagination_links(resource):
    """ Returns the appropriate set of resource links depending on the
    current page and the total number of documents returned by the query.
    """

    links = {}
    params = get_context()
    next_offset = params.offset + params.limit
    links['next'] = {'href': '/%s?limit=%s&offset=%s' %\
                                (resource, params.limit, next_offset)}

    return links


def _prep_query_(schema, query):

    if query is None:
        return None

    allowed = all(k in ALLOWED for k in query.keys())
    if not allowed:
        abort(400)

    def get_type(field, schema):
        _type = schema[field]['type']
        if _type == 'list':
            return schema[field]['schema']['type']
        return _type

    for field, val in query.iteritems():
        _type = get_type(field, schema)

        if isinstance(val, dict):
            operators = val.keys()
            if operator not in COMPARISON:
                abort(400)


def _prep_query(query):
    """ Prepare mongodb query """

    if query is None:
        return None

    def convert_objects(q):
        if isinstance(q, list):
            return [convert_objects(item) for item in q]

        for key, val in q.iteritems():
            if isinstance(val, dict):
                q[key] = convert_objects(val)
            elif isinstance(val, basestring) and re.match(r"^[0-9a-fA-F]{24}$", val):
                # @TODO This also matches strings that look like object id's
                q[key] = ObjectId(val)
            elif isinstance(val, basestring) and re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                q[key] = datetime.strptime(val, "%Y-%m-%d")
            elif isinstance(val, basestring) and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", val):
                q[key] = datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")
            elif isinstance(val, basestring) and re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\w{1,3}$", val):
                # explicit timezone handling
                # datetime tz handling is not platform independent, should use other package, val = val.replace(' ', '+')
                # @TODO for now only support UTC/Z flags for tz
                if val[-1] == 'Z': val = val[:-1] + 'UTC'
                q[key] = datetime.strptime(val, "%Y-%m-%dT%H:%M:%S%Z")
        return q

    return convert_objects(query)

def get_context():
    """ Retreive the URL parameters from the current request context """

    args = request.args
    int_or_none = lambda x: int(x) if x is not None else None
    dict_or_none = lambda x: json.loads(x) if x is not None else None

    try:
        context = RequestContext(
            int_or_none(args.get('limit')) or 25,
            int_or_none(args.get('offset')) or 0,
            _prep_query(dict_or_none(args.get('q'))),
            dict_or_none(args.get('embedded')),
            dict_or_none(args.get('projection')),
            dict_or_none(args.get('sort')),
            _prep_query(dict_or_none(args.get('aggregate'))),
        )
        pre_fetch.send(context)
        return context
    except Exception as e:
        abort(400, e)

