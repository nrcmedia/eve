articles = {

    'datasource': {
        'source': 'articles',
    },

    'hateoas': True,

    'schema': {

        'title': {
            'type': 'string',
            'required': True,
        },

        'created_at': {
            'type': 'datetime',
        },

        'is_published': {
            'type': 'boolean',
        },

        'is_free': {
            'type': 'boolean',
        },

        'modified_at': {
            'type': 'datetime',
        },

        'derivative_of': {
            'type': 'objectid',
            'data_relation': {
                 'collection': 'articles',
                 'field': '_id',
             },
             'nullable': True,
        },


        'authors': {
            'nullable': True,
            'type': 'list',
            'schema': {
                'type': 'objectid',
                'data_relation': {
                    'collection': 'authors',
                    'field': '_id',
                    'embeddable': True
                }
            }
        },

        'images': {
            'nullable': True,
            'type': 'list',
            'schema': {
                'type': 'objectid',
                'data_relation': {
                    'collection': 'images',
                    'field': '_id',
                    'embeddable': True
                }
            }
        },

    }
}


images = {
    'datasource': {
        'source': 'images',
    },

    'hateoas': True,

    'schema': {
        'is_featured': {
            'type': 'boolean',
            'nullable': True,
        },
        'url': {
            'type': 'string',
        },
        'caption': {
            'type': 'string',
            'nullable': True,
        },
        'width': {
            'type': 'integer',
            'nullable': True,
        },
        'height': {
            'type': 'integer',
            'nullable': True,
        },
        'md5': {
            'type': 'string',
            'nullable': True,
        },
        'name': {
            'type': 'string',
            'nullable': True,
        },
    }
}

authors = {
    'datasource': {
        'source': 'authors',
    },

    'hateoas': True,

    'schema': {
        'name': {
            'type': 'string',
        },
        'slug': {
            'type': 'string',
            'required': True,
            'unique': True
        },
        'social': {
            'type': 'dict',
            'nullable': True,
        }
    }
}
