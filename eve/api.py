from flask import Blueprint
from eve import default_settings
from routes import ApiView
from flask.ext.pymongo import PyMongo
from auth import contruct_auth

class Api(object):
    """ The main Api object, on init it will add routes for the configured urls to create the
    endpoints, wrapped in a Blueprint """

    def __init__(self, url_prefix, app=None, auth=None):
        self.app = app
        if app is not None:
            self.init_app(app, url_prefix, auth)

    def init_app(self, app, url_prefix, auth):
        self.driver = PyMongo(app)

        # Set default api configuration, if not already set by users config
        for key in dir(default_settings):
            app.config.setdefault(key, getattr(default_settings, key))

        blueprint = Blueprint('eve', 'eve', url_prefix=url_prefix)

        def register_api(resource, endpoint, url, pk='_id', pk_type='ObjectId', auth=None):
            if auth:
                # add Basic Auth
                ApiView.decorators = [contruct_auth(auth)]

            view_func = ApiView.as_view(endpoint, self.driver, resource)
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
