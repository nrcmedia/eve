import datetime
from flask import current_app as app, url_for
import json
from eve.render import APIEncoder


def document_link(resource, doc_id=None):
    """ Generate an URI for a resource endpoint or individual items """
    url = url_for('eve.%s_api' % resource)
    if doc_id:
        url += str(doc_id)
    return url


def str_to_date(string):
    """ Converts a RFC-1123 string to the corresponding datetime value.

    :param string: the RFC-1123 string to convert to datetime value.
    """
    # @TODO Try this and raise value error when it fails
    return datetime.strptime(string, app.config['DATE_FORMAT']) if string else None


def jsonify(doc):
    """ Flasks jsonify with a twist, takes document instances and
    renders BSON types to JSON (dates and objectid's) """

    indent = None
    return app.response_class(json.dumps(doc,
        indent=indent, cls=APIEncoder), mimetype='application/json',
        content_type='application/json')
