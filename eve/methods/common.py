# -*- coding: utf-8 -*-

"""
    eve.methods.common
    ~~~~~~~~~~~~~~~~~~

    Utility functions for API methods implementations.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

import time
from datetime import datetime
from collections import namedtuple
from werkzeug.exceptions import default_exceptions, HTTPException
from flask import current_app as app, abort as flask_abort, request, g, Response, make_response, url_for, Response
from flask.views import MethodView
import simplejson as json
from ..utils import parse_request, document_etag, config, \
    request_method, debug_error_message
from bson.objectid import ObjectId
from functools import wraps
from eve.render import APIEncoder
from eve.io.mongo import Validator
from bson.errors import InvalidId
from dateutil.tz import tzlocal

Context = namedtuple('Context', 'limit offset query embedded projection')

def str_to_date(string):
    """ Converts a RFC-1123 string to the corresponding datetime value.

    :param string: the RFC-1123 string to convert to datetime value.
    """
    return datetime.strptime(string, app.config['DATE_FORMAT']) if string else None

def document_link(resource, doc_id=None):
    """ Generate an URI for a resource endpoint or individual items """
    url = url_for('eve.%s_api' % resource)
    if doc_id:
        url += str(doc_id)
    return url

def _find_objectid_fields(schema):
    """ Find object id fields in schema """

    related_keys = {}
    for key, val in schema.iteritems():
        if val['type'] == 'objectid':
            related_keys[key] = {'resource': val['data_relation']['collection'], 'multi': False}
        elif val['type'] == 'list' and val['schema']['type'] == 'objectid':
            related_keys[key] = {'resource': val['schema']['data_relation']['collection'], 'multi': True}

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

    memoize = {}

    for document in documents:
        for field in enabled_embedded_fields:
            # Retrieve and serialize the requested document
            if not document.get(field):
                continue

            field_definition = config['DOMAIN'][resource]['schema'][field]

            if type(document[field]) is list:
                key = "".join([str(_id) for _id in document[field]])
                if not memoize.get(key):
                    query = {'$or': [
                        {'_id': id_} for id_ in document[field]
                    ]}
                    memoize[key] = [doc for doc in db[field_definition['schema']['data_relation']['collection']].find(
                        query
                    )]
                embedded_doc = memoize[key]
            else:
                if not memoize.get(document[field]):
                    memoize[document[field]] = db[field_definition['data_relation']['collection']].find_one(
                        **{'_id': document[field]}
                    )
                embedded_doc = memoize[document[field]]

            if embedded_doc:
                document.setdefault("_embedded", {})
                document['_embedded'][field] = embedded_doc
                del document[field]


class JSONHTTPException(HTTPException):
    """A base class for HTTP exceptions with ``Content-Type:
    application/json``.

    The ``description`` attribute of this class must set to a string (*not* an
    HTML string) which describes the error.

    """

    def get_body(self, environ):
        """Overrides :meth:`werkzeug.exceptions.HTTPException.get_body` to
        return the description of this error in JSON format instead of HTML.

        """
        return json.dumps(dict(description=self.get_description(environ)))

    def get_headers(self, environ):
        """Returns a list of headers including ``Content-Type:
        application/json``.

        """
        return [('Content-Type', 'application/json')]


def abort(status_code, body=None, headers={}):
    """
    Content negiate the error response.
    """

    if 'text/html' in request.headers.get("Accept", ""):
        error_cls = HTTPException
    else:
        error_cls = JSONHTTPException

    class_name = error_cls.__name__
    bases = [error_cls]
    attributes = {'code': status_code}

    if status_code in default_exceptions:
        # Mixin the Werkzeug exception
        bases.insert(0, default_exceptions[status_code])

    error_cls = type(class_name, tuple(bases), attributes)
    flask_abort(make_response(error_cls(body), status_code, headers))


def jsonify(doc):
    """ Flasks jsonify with a twist, takes document instances and
    renders BSON types to JSON (dates and objectid's) """

    indent = None
    if app.config['JSONIFY_PRETTYPRINT_REGULAR'] \
       and not request.is_xhr:
        indent = 2
    return app.response_class(json.dumps(doc,
        indent=indent, cls=APIEncoder), mimetype='application/json')


def _pagination_links(resource):
    """Returns the appropriate set of resource links depending on the
    current page and the total number of documents returned by the query.

    :param resource: the resource name.
    :param req: and instace of :class:`eve.utils.ParsedRequest`.
    :param document_count: the number of documents returned by the query.
    """
    #_links = {'parent': home_link(), 'self': collection_link(resource)}

    links = {}
    params = get_context()
    next_offset = params.offset + params.limit
    links['next'] = {'href': '/%s?limit=%s&offset=%s' %\
                                (resource, params.limit, next_offset)}

    return links


def get_context():
    """ Retreive the URL parameters from the current request context """

    args = request.args
    int_or_none = lambda x: int(x) if x is not None else None
    dict_or_none = lambda x: json.loads(x) if x is not None else None

    try:
        return Context(
            int_or_none(args.get('limit')) or 25,
            int_or_none(args.get('offset')) or 0,
            dict_or_none(args.get('q')),
            dict_or_none(args.get('embedded')),
            dict_or_none(args.get('projection'))
        )
    except ValueError:
        abort(400, {'error': '{limit} and {offset} must be integers'})


class ApiView(MethodView):
    """ Pluggable view for RESTful crud operations on mongo collections """

    pk = '_id'

    def __init__(self, driver, resource, *args, **kwargs):
        """ Calls the init of the superclass and specifies the resource to which
        this view provides the API endpoints """

        super(ApiView, self).__init__(*args, **kwargs)
        self.db = driver.db
        self.resource = resource
        self.collection = driver.db[resource]
        self.resource_def = app.config['DOMAIN'][resource]
        self.schema = app.config['DOMAIN'][resource]['schema']
        self.validator = Validator(self.schema, self.db, resource)
        self.reference_keys = _find_objectid_fields(self.schema)
        self.date_keys = _find_date_fields(self.schema)

    def get(self, **kwargs):
        """ GET requests """

        if all(v is None for v in kwargs.values()):
            params = get_context()

            find_args = []
            if params.query:
                find_args.append(params.query)
            if params.projection:
                if not len(find_args):
                    find_args.append({})
                find_args.append(params.projection)

            cursor = self.collection.find(*find_args)\
                        .skip(params.offset).limit(params.limit)

            documents = []
            for doc in cursor:
                # Calc hash for etag first
                doc['etag'] = document_etag(doc)
                # Add links to referenced resources
                _add_links(doc, self.reference_keys, self.resource)
                documents.append(doc)

            # Embedded documents
            if params.embedded:
                _resolve_embedded_documents(self.resource, self.db, params.embedded, documents)
            # Add paginated links
            links = _pagination_links(self.resource)

            return jsonify({'_items': documents, '_links': links}), 200

        else:
            # Item endpoint
            if not '_id' in kwargs:
                abort(400, {'error': 'only {_id} is supported'})
            try:
                _id = ObjectId(kwargs['_id'])
            except (InvalidId, TypeError):
                abort(400, {'error': 'Invalid id'})

            none_match = request.headers.get('If-None-Match')
            doc = self.db[self.resource].find_one({'_id': _id})

            if doc:
                doc['etag'] = document_etag(doc)
                if none_match and none_match.replace('"','') == doc.get('etag'):
                    abort(304)
                _add_links(doc, self.reference_keys, self.resource)
                resp = jsonify(doc)
                if doc.get('updated_at'):
                    resp.headers.set('Last-Modified',
                        doc['updated_at'].strftime("%a, %d %b %Y %H:%M:%S %Z"))

                return resp, 200

            abort(404)

    def post(self):
        """ POST request """

        payload = request.get_json(force=True)
        doc = self._parse(payload)
        # Add updated/created fields to doc
        self._add_timers(doc)

        if '_embedded' in doc:
            for key, embed in doc['_embedded'].items():
                resource = self.reference_keys[key]['resource']
                _id = get_or_create(self.db[resource], resource, embed)

        validated = self.validator.validate(doc)
        if not validated:
            return jsonify({'errors': self.validator.errors}), 400

        _id = self.collection.insert(doc)
        if _id:
            doc = {'_id': _id}
            _add_links(doc, {}, self.resource)
            resp = jsonify(doc)
            # Add Location header
            resp.headers.set('Location', document_link(self.resource, _id))
            return resp, 201

    def patch(self, **kwargs):
        """ PATCH request """

        payload = request.get_json(force=True)

        if '_id' not in kwargs:
            abort(400, {'error': 'Please provide an {_id}'})

        doc = self._parse(payload)
        validated = self.validator.validate(doc, update=True)
        if not validated:
            return jsonify({'errors': self.validator.errors}), 400

        result = self.collection.update({'_id': kwargs['_id']}, doc)
        if result.get('updatedExisting') == True:
            # Succesful patch requests return 204 with a pointer to the
            # updated resource.
            resp = jsonify({})
            resp.headers.set('Content-Location', document_link(self.resource, kwargs['_id']))
            return resp, 204

    def _parse(self, payload):
        """ Parse incoming payloads """

        # Object id field strings to ObjectId instances
        if payload.get('_id'):
            payload['_id'] = ObjectId(payload['_id'])

        doc_references = [k for k in self.reference_keys if k in payload]
        try:
            for obj_key in doc_references:
                if self.reference_keys[obj_key].get('multi'):
                    payload[obj_key] = [ObjectId(v) for v in payload[obj_key]]
                else:
                    payload[obj_key] = ObjectId(payload[obj_key])
        except (InvalidId, TypeError):
            abort(400, {'error': 'Reference fields should be 24 char hex strings'})

        # Date strings to datetime objects
        doc_dates = self.date_keys.intersection(payload.keys())
        for date_key in doc_dates:
            payload[date_key] = str_to_date(payload[date_key])

        return payload

    def _add_timers(self, doc):
        doc['updated_at'] = datetime.now(tzlocal())
        doc['created_at'] = datetime.now(tzlocal())

def get_or_create(collection, resource, payload):
    schema = app.config['DOMAIN'][resource]['schema']
    uniques = [key for key in schema if schema[key].get('unique')]

    if all(k in payload for k in uniques):
        uniq_values = dict( (k, v) for (k, v) in payload.iteritems()
                        if k in uniques)

        doc = collection.find_one(uniq_values, {})
        if doc:
            return doc

    collection.insert(payload)



def get_document(resource, **lookup):
    """ Retrieves and return a single document. Since this function is used by
    the editing methods (POST, PATCH, DELETE), we make sure that the client
    request references the current representation of the document before
    returning it.

    :param resource: the name of the resource to which the document belongs to.
    :param **lookup: document lookup query

    .. versionchanged:: 0.0.9
       More informative error messages.

    .. versionchanged:: 0.0.5
      Pass current resource to ``parse_request``, allowing for proper
      processing of new configuration settings: `filters`, `sorting`, `paging`.
    """
    #req = parse_request(resource)
    document = app.data.find_one(resource, **lookup)
    if document:

        #if not req.if_match:
        #    # we don't allow editing unless the client provides an etag
        #    # for the document
        #    abort(403, description=debug_error_message(
        #        'An etag must be provided to edit a document'
        #    ))

        # ensure the retrieved document has LAST_UPDATED and DATE_CREATED,
        # eventually with same default values as in GET.
        document[config.LAST_UPDATED] = last_updated(document)
        document[config.DATE_CREATED] = date_created(document)

        #if req.if_match != document_etag(document):
        #    # client and server etags must match, or we don't allow editing
        #    # (ensures that client's version of the document is up to date)
        #    abort(412, description=debug_error_message(
        #        'Client and server etags don\'t match'
        #    ))

    return document


def parse(value, resource):
    """ Safely evaluates a string containing a Python expression. We are
    receiving json and returning a dict.

    :param value: the string to be evaluated.
    :param resource: name of the involved resource.

    .. versionchanged:: 0.1.0
       Support for PUT method.

    .. versionchanged:: 0.0.5
       Support for 'application/json' Content-Type.

    .. versionchanged:: 0.0.4
       When parsing POST requests, eventual default values are injected in
       parsed documents.
    """

    try:
        # assume it's not decoded to json yet (request Content-Type = form)
        document = json.loads(value)
        #except (TypeError, ValueError, OverflowError), exception:
        #    current_app.logger.exception(exception.message)
        #   return jsonify_status_code(400, message='Unable to decode data')
    except:
        # already a json
        document = value

    # By design, dates are expressed as RFC-1123 strings. We convert them
    # to proper datetimes.
    dates = app.config['DOMAIN'][resource]['dates']
    document_dates = dates.intersection(set(document.keys()))
    for date_field in document_dates:
        document[date_field] = str_to_date(document[date_field])

    # update the document with eventual default values
    if request_method() in ('POST', 'PUT'):
        defaults = app.config['DOMAIN'][resource]['defaults']
        missing_defaults = defaults.difference(set(document.keys()))
        schema = config.DOMAIN[resource]['schema']
        for missing_field in missing_defaults:
            document[missing_field] = schema[missing_field]['default']

    return document


def payload():
    """ Performs sanity checks or decoding depending on the Content-Type,
    then returns the request payload as a dict. If request Content-Type is
    unsupported, aborts with a 400 (Bad Request).

    .. versionchanged:: 0.0.9
       More informative error messages.
       request.get_json() replaces the now deprecated request.json


    .. versionchanged:: 0.0.7
       Native Flask request.json preferred over json.loads.

    .. versionadded: 0.0.5
    """
    content_type = request.headers['Content-Type'].split(';')[0]

    if content_type == 'application/json':
        return request.get_json()
    elif content_type == 'application/x-www-form-urlencoded':
        return request.form if len(request.form) else \
            abort(400, description=debug_error_message(
                'No form-urlencoded data supplied'
            ))
    else:
        abort(400, description=debug_error_message(
            'Unknown or no Content-Type header supplied'))


class RateLimit(object):
    """ Implements the Rate-Limiting logic using Redis as a backend.

    :param key_prefix: the key used to uniquely identify a client.
    :param limit: requests limit, per period.
    :param period: limit validity period
    :param send_x_headers: True if response headers are supposed to include
                           special 'X-RateLimit' headers

    .. versionadded:: 0.0.7
    """
    # We give the key extra expiration_window seconds time to expire in redis
    # so that badly synchronized clocks between the workers and the redis
    # server do not cause problems
    expiration_window = 10

    def __init__(self, key_prefix, limit, period, send_x_headers=True):
        self.reset = (int(time.time()) // period) * period + period
        self.key = key_prefix + str(self.reset)
        self.limit = limit
        self.period = period
        self.send_x_headers = send_x_headers
        p = app.redis.pipeline()
        p.incr(self.key)
        p.expireat(self.key, self.reset + self.expiration_window)
        self.current = min(p.execute()[0], limit + 1)

    remaining = property(lambda x: x.limit - x.current)
    over_limit = property(lambda x: x.current > x.limit)


def get_rate_limit():
    """ If available, returns a RateLimit instance which is valid for the
    current request-response.

    .. versionadded:: 0.0.7
    """
    return getattr(g, '_rate_limit', None)


def ratelimit():
    """ Enables support for Rate-Limits on API methods
    The key is constructed by default from the remote address or the
    authorization.username if authentication is being used. On
    a authentication-only API, this will impose a ratelimit even on
    non-authenticated users, reducing exposure to DDoS attacks.

    Before the function is executed it increments the rate limit with the help
    of the RateLimit class and stores an instance on g as g._rate_limit. Also
    if the client is indeed over limit, we return a 429, see
    http://tools.ietf.org/html/draft-nottingham-http-new-status-04#section-4

    .. versionadded:: 0.0.7
    """
    def decorator(f):
        @wraps(f)
        def rate_limited(*args, **kwargs):
            method_limit = app.config.get('RATE_LIMIT_' + request_method())
            if method_limit and app.redis:
                limit = method_limit[0]
                period = method_limit[1]
                # If authorization is being used the key is 'username'.
                # Else, fallback to client IP.
                key = 'rate-limit/%s' % (request.authorization.username
                                         if request.authorization else
                                         request.remote_addr)
                rlimit = RateLimit(key, limit, period, True)
                if rlimit.over_limit:
                    return Response('Rate limit exceeded', 429)
                # store the rate limit for further processing by
                # send_response
                g._rate_limit = rlimit
            else:
                g._rate_limit = None
            return f(*args, **kwargs)
        return rate_limited
    return decorator


def last_updated(document):
    """Fixes document's LAST_UPDATED field value. Flask-PyMongo returns
    timezone-aware values while stdlib datetime values are timezone-naive.
    Comparisions between the two would fail.

    If LAST_UPDATE is missing we assume that it has been created outside of the
    API context and inject a default value, to allow for proper computing of
    Last-Modified header tag. By design all documents return a LAST_UPDATED
    (and we don't want to break existing clients).

    :param document: the document to be processed.

    .. versionchanged:: 0.1.0
       Moved to common.py and renamed as public, so it can also be used by edit
       methods (via get_document()).

    .. versionadded:: 0.0.5
    """
    if config.LAST_UPDATED in document:
        return document[config.LAST_UPDATED].replace(tzinfo=None)
    else:
        return epoch()


def date_created(document):
    """If DATE_CREATED is missing we assume that it has been created outside of
    the API context and inject a default value. By design all documents
    return a DATE_CREATED (and we dont' want to break existing clients).

    :param document: the document to be processed.

    .. versionchanged:: 0.1.0
       Moved to common.py and renamed as public, so it can also be used by edit
       methods (via get_document()).

    .. versionadded:: 0.0.5
    """
    return document[config.DATE_CREATED] if config.DATE_CREATED in document \
        else epoch()


def epoch():
    """ A datetime.min alternative which won't crash on us.

    .. versionchanged:: 0.1.0
       Moved to common.py and renamed as public, so it can also be used by edit
       methods (via get_document()).

    .. versionadded:: 0.0.5
    """
    return datetime(1970, 1, 1)
