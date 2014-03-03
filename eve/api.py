from flask import Blueprint, Request
from eve import default_settings
from eve.errors import abort
from eve.helpers import jsonify, document_link
from routes import ApiView
from flask.ext.pymongo import PyMongo
from auth import contruct_auth
import logging

logger = logging.getLogger('mongrest')

class JSONRequest(Request):
    def on_json_loading_failed(self, e):
        abort(400, 'Not supported mime type or invalid message')


class Api(object):
    """ The main Api object, on init it will add routes for the configured urls to create the
    endpoints, wrapped in a Blueprint """


    def __init__(self, url_prefix, app=None, auth=None):
        self.app = app
        if app is not None:
            self.init_app(app, url_prefix, auth)


    def init_app(self, app, url_prefix, auth):
        self.db = PyMongo(app)
        app.db = self.db
        app.request_class = JSONRequest

        # Set default api configuration, if not already set by users config
        for key in dir(default_settings):
            app.config.setdefault(key, getattr(default_settings, key))

        blueprint = Blueprint('eve', 'eve', url_prefix=url_prefix)

        if not app.debug:
            @blueprint.app_errorhandler(Exception)
            def unhandled_exception(e):
                logger.exception(e)
                abort(500)

        def register_api(resource, endpoint, url, pk='_id', pk_type='ObjectId', auth=None):
            if auth:
                # add Basic Auth
                ApiView.decorators = [contruct_auth(auth)]

            view_func = ApiView.as_view(endpoint, self.db, resource)
            blueprint.add_url_rule(url, defaults={pk: None},
                                    view_func=view_func, methods=['GET',])
            blueprint.add_url_rule(url, view_func=view_func, methods=['POST',])
            blueprint.add_url_rule('%s<%s:%s>' % (url, pk_type, pk), view_func=view_func,
                                             methods=['GET', 'PATCH', 'DELETE', 'PUT'])

        for resource, settings in app.config['DOMAIN'].items():
            base_url = "/%s/" % resource
            register_api(resource, "%s_api" % resource, base_url, pk='_id', auth=auth)

        self.blueprint = blueprint
        app.register_blueprint(blueprint)
