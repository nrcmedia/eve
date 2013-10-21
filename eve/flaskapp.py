# -*- coding: utf-8 -*-

"""
    eve.flaskapp
    ~~~~~~~~~~~~

    This module implements the central WSGI application object as a Flask
    subclass.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

import eve
from flask import Flask, Blueprint
from werkzeug.routing import BaseConverter
from werkzeug.serving import WSGIRequestHandler
from eve import default_settings
from eve.io.mongo import Mongo, Validator
from eve.methods.common import ApiView
from flask.ext.pymongo import PyMongo


class EveWSGIRequestHandler(WSGIRequestHandler):
    """ Extend werkzeug request handler to include current Eve version in all
    responses, which is super-handy for debugging.
    """
    @property
    def server_version(self):
        return 'Eve/%s ' % eve.__version__ + super(EveWSGIRequestHandler,
                                                   self).server_version

class RegexConverter(BaseConverter):
    """ Extend werkzeug routing by supporting regex for urls/API endpoints """
    def __init__(self, url_map, *items):
        super(RegexConverter, self).__init__(url_map)
        self.regex = items[0]


class Api(object):
    """ The main Api object, on init it will add all the configured urls to create the
    endpoints (wrapped in a Blueprint) """

    def __init__(self, url_prefix, app=None):
        self.app = app
        if app is not None:
            self.init_app(app, url_prefix)

    def init_app(self, app, url_prefix):
        self.driver = PyMongo(app)

        # Set default api configuration, if not already set by users config
        for key in dir(default_settings):
            app.config.setdefault(key, getattr(default_settings, key))

        blueprint = Blueprint('eve', 'eve', url_prefix=url_prefix)

        resources = {}
        urls = {}
        datasources = {}

        #blueprint.add_url_rule('/', 'home', view_func=home_endpoint)

        def register_api(resource, endpoint, url, pk='_id', pk_type='ObjectId'):
            view_func = ApiView.as_view(endpoint, self.driver, resource)
            blueprint.add_url_rule(url, defaults={pk: None},
                                    view_func=view_func, methods=['GET',])
            blueprint.add_url_rule(url, view_func=view_func, methods=['POST',])
            blueprint.add_url_rule('%s<%s:%s>' % (url, pk_type, pk), view_func=view_func,
                                             methods=['GET', 'PATCH', 'DELETE'])

        for resource, settings in app.config['DOMAIN'].items():
            base_url = "/%s/" % resource
            register_api(resource, "%s_api" % resource, base_url, pk='_id')

        self.blueprint = blueprint

        app.config['RESOURCES'] = resources
        app.config['URLS'] = urls
        app.config['SOURCES'] = datasources

        app.register_blueprint(blueprint)
