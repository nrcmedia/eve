import logging
from eve.tests import test_schema

logger = logging.getLogger('test')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

DEBUG = True
LOGGER_NAME = 'test'

SERVER_NAME = None

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DBNAME = 'test'

LAST_UPDATED = 'modified_at'
DATE_CREATED = 'created_at'

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

HATEOAS = True

DOMAIN = {}
DOMAIN['articles'] = test_schema.articles
DOMAIN['authors'] = test_schema.authors
DOMAIN['images'] = test_schema.images

RESOURCE_METHODS = ['GET', 'POST']
ITEM_METHODS = ['GET', 'PATCH', 'DELETE', 'PUT']