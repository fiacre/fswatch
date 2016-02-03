import json
import sys
from fs_config import FSConfig
from requests.auth import HTTPBasicAuth
import logging
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

logger = logging.getLogger(__name__)


class SearchAppIndexException(Exception):
    pass

SERVICE_PATHS = {
    'index': '_search/{real_app}/{schema}',
    'mapping': '_search/{app}/_mapping/{schema}'
}

conf_file = './fswatch.conf'
config = FSConfig(conf_file)


class SearchAppApi(object):
    """
    interface to searchApp API endpoint
    assumes:
    an ES search app has been created, app-key, search-user key have been generated
    methods:
    get_auth
    get_index
    do_post
    init_schema
    """
    def __init__(self, search_key, app_key, base_url=None, schema=None):
        if not search_key or app_key:
            if not search_key:
                raise SearchAppIndexException('No customer key provided')
            if not app_key:
                raise SearchAppIndexException('No app name provided')

        if base_url:
            self.base_url = base_url
        else:
            self.base_url = config.base_url()

        if schema:
            self.schema = schema
        else:
            self.schema = config.schema()

        self.search_key = search_key
        self.app_key = app_key

    def get_real_app_name(self):
        """
        :return: the real_app name (app is created before watcher can run)
        """
        return 'default'

    def get_auth(self):
        """
        :return: AUTH headers for this app and search-user
        """
        return HTTPBasicAuth(self.search_key, self.app_key)

    def get_index_url(self):
        """
        :return: URL for indexing documents
        """
        real_app_name = self.get_real_app_name()
        url = "{location}/_search/{app_name}/{schema}".format(
                location=self.base_url,
                app_name=real_app_name,
                schema=self.schema
        )
        return url

    def init_schema(self):
        """
        The first time the watcher runs
        want to initialize an ES mapping
        These five attributes (filename,
        file_owner, file_modified, file_perms
        and file size are general metadata
        that will persist in the ES schema
        to which the watcher posts
        """
        init_schema_data = {
            "title": "filename",
            "display": "file_owner",

            "datatypes": {
                "filename" : {
                "type": "string",
                "is_search" : True,
                "is_facet": True
            },
            "file_owner": {
                "type": "string",
                "is_search": True,
                "is_facet": True
            },
            "file_modified":{
                "type": "date"
            },
            "file_perms": {
                "type": "string",
                "is_facet": False
            },
            "file_size": {
                "type": "integer",
                "is_facet": False
                }
            }
        }
        # mapping endpoint
        endpoint = "{location}/_search/default/_mapping/{schema}".format(location=self.base_url, schema=self.schema)
        req = requests.post(endpoint, auth=self.get_auth(), data=json.dumps(init_schema_data))
        if req.status_code != 200:
            raise requests.RequestException("Could not initialize schema {}".format(req.reason()))

    def do_post(self, filename, mimetype, owner, modified, perms, size):
        """
        Got yer meat and potatoes here
        :param filename: name of file to be uploaded
        :param mimetype: and its mimetype
        :return:
        """
        auth = self.get_auth()
        index_url = self.get_index_url()
        multipart_data = MultipartEncoder(
            fields={
                'file': (filename, open(filename, 'rb'), mimetype),
                'owner': owner,
                'modified': str(modified)
                # 'file_perms': perms,
                # 'file_size': "{0} bytes".format(size)
            })
        resp = requests.post(
                index_url, auth=auth, data=multipart_data, headers={'Content-Type': multipart_data.content_type}
        )
        logger.debug(resp.status_code)
        if resp.status_code != 200:
            raise SearchAppIndexException("{0} {1} {2}".format(resp.status_code, resp.reason, resp.url))
        resp.close()

# local sanity test
if __name__ == '__main__':
    api = SearchAppApi(config.search_key(), config.app_key(), config.base_url(), config.schema())
    api.init_schema()
    api.do_post("files/TestMe.pdf", 'application/pdf')
