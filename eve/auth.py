from flask import request
from functools import wraps
from errors import abort

def check_auth(username, password):
    """ This function is called to check if a username /
    password combination is valid.
    """
    return username == 'crn' and password == 'crn'


def authenticate():
    """ Sends a 401 response that enables basic auth"""
    abort(401, "Please use the proper credentials", {'WWW-Authenticate':'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


def set_auth(methods, decorator):
    """ Decorate all the views """
    def decorate(cls):
        for attr in cls.__dict__: # there's propably a better way to do this
            if attr in methods:
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate
