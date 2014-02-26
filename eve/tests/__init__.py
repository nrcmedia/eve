import unittest
#import test_settings
from flask import Flask
from eve import Api
from eve.tests import test_settings
import random
import string
import json
from pymongo import MongoClient
from pprint import pprint

mongo_client = MongoClient(test_settings.MONGO_HOST, test_settings.MONGO_PORT)

class TestMinimal(unittest.TestCase):
    """ This stuff should probably work
    """

    def tearDown(self):
        """ Drop database, without application context """
        mongo_client[self.app.config['MONGO_DBNAME']].drop_collection('articles')
        mongo_client[self.app.config['MONGO_DBNAME']].drop_collection('authors')
        mongo_client[self.app.config['MONGO_DBNAME']].drop_collection('images')

    def setUp(self):
        """ Prepare the test fixture

        :param settings_file: the name of the settings file.  Defaults
                              to `eve/tests/test_settings.py`.
        """
        self.prefix = '/test'
        self.app = Flask(__name__)
        self.app.config.from_object(test_settings)
        self.app.testing = True
        self.api = Api(self.prefix, self.app)
        self.test_client = self.app.test_client()

        #self.dummy_insert()

    def dummy_insert(self):
        db = mongo_client[self.app.config['MONGO_DBNAME']]
        author = dict(
            slug='piet',
            name='Piet',
            social=dict(
                shalala='lalala'
            )
        )
        _id = db.authors.insert(author)
        article = dict(
            title='Article with author',
            authors=[_id]
        )
        return db.articles.insert(article)

    def random_string(self, num):
        return (''.join(random.choice(string.ascii_lowercase)
                        for x in range(num)))


    def random_articles(self, num):
        lst = []
        for i in xrange(num):
            article = dict(
                title=self.random_string(),
                published_at=''
            )
            lst.append(article)


    def post(self, url, data, content_type='application/json'):
        url = '%s%s' % (self.prefix, url)
        if content_type == 'application/json':
            data = json.dumps(data)
        return self.test_client.post(
            url, data=data, content_type=content_type
        )

    def patch(self, url, data, content_type='application/json'):
        url = '%s%s' % (self.prefix, url)
        if content_type == 'application/json':
            data = json.dumps(data)
        return self.test_client.patch(
            url, data=data, content_type=content_type
        )


    def parse_response(self, r):
        """ Response bodies should be JSON """
        try:
            return json.loads(r.get_data())
        except ValueError:
            return None

    def test_bad_json(self):
        endpoint = '/articles/'
        data = '{noquotes: true}'
        r = self.post(endpoint, data)

        self.assertEqual(r.status_code, 400 )
        response = self.parse_response(r)
        self.assertTrue(response is not None)


    def test_post_bad_content_type(self):
        endpoint = '/articles/'
        data = 'aap=noot&mies=wim'
        r = self.post(endpoint, data,
                    content_type='application/x-www-form-urlencoded')
        self.assertEqual(r.status_code, 400)
        response = self.parse_response(r)
        self.assertTrue(response is not None)


    def test_post_known_endpoint(self):
        endpoint = '/articles/'
        data = dict(
            title='My First Article!',
        )
        r = self.post(endpoint, data)

        self.assertEqual(r.status_code, 201)
        newinstance = self.parse_response(r)
        self.assertTrue(newinstance is not None)
        self.assertEqual(newinstance['title'], data['title'])


    def test_post_embedded_new(self):
        endpoint = '/articles/'
        data = dict(
            title='Article with relations',
            _embedded = dict(
                authors= [
                    dict(
                        slug='piet',
                        name='Piet',
                        social=dict(
                            shalala='lalala'
                        )
                    ),
                ]
            )
        )
        r = self.post(endpoint, data)
        response = self.parse_response(r)

        self.assertTrue(response is not None)
        self.assertTrue(response['_links']['authors'][0]['href'] is not None)

        r = self.test_client.get(response['_links']['authors'][0]['href'])
        embed = self.parse_response(r)
        self.assertEqual(embed['name'], data['_embedded']['authors'][0]['name'])

    def test_patch_replace_single_field_on_embedded(self):
        _id = self.dummy_insert()
        data = [dict(
            op='replace',
            path='/_embedded/authors/0/name',
            value='Jantje'
        )]
        r = self.patch('/articles/%s' % _id, data)
        self.assertEqual(r.status_code, 204)
        r = self.test_client.get('/test/articles/%s?embedded={"authors":1}' % _id)
        response = self.parse_response(r)
        self.assertEqual(data[0]['value'], response['_embedded']['authors'][0]['name'])

if __name__ == '__main__':
    unittest.main()