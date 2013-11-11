# -*- coding: utf-8 -*-

"""
    eve.flaskapp
    ~~~~~~~~~~~~

    This module implements the central WSGI application object as a Flask
    subclass.

    :copyright: (c) 2013 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

from flask import Blueprint
from eve import default_settings
from routes import ApiView
from flask.ext.pymongo import PyMongo

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

        #blueprint.add_url_rule('/', 'home', view_func=home_endpoint)

        def register_api(resource, endpoint, url, pk='_id', pk_type='ObjectId'):
            view_func = ApiView.as_view(endpoint, self.driver, resource)
            blueprint.add_url_rule(url, defaults={pk: None},
                                    view_func=view_func, methods=['GET',])
            blueprint.add_url_rule(url, view_func=view_func, methods=['POST',])
            blueprint.add_url_rule('%s<%s:%s>' % (url, pk_type, pk), view_func=view_func,
                                             methods=['GET', 'PATCH', 'DELETE', 'PUT'])

        for resource, settings in app.config['DOMAIN'].items():
            base_url = "/%s/" % resource
            register_api(resource, "%s_api" % resource, base_url, pk='_id')

        self.blueprint = blueprint
        app.register_blueprint(blueprint)
