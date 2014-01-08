from flask import request
from functools import wraps
from errors import abort

def contruct_auth(auth_check):
    def requires_auth(f):
        @wraps(f)
        def decorater(*args, **kwargs):
            auth = request.authorization
            if not auth or not auth_check(auth.username, auth.password):
                return authenticate()
            return f(*args, **kwargs)
        return decorater

    return requires_auth

def authenticate():
    """ Sends a 401 response that enables basic auth"""
    abort(401, "Please use the proper credentials", {'WWW-Authenticate':'Basic realm="Login Required"'})