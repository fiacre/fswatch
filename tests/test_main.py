import unittest
from unittest.mock import patch
from unittest.mock import Mock
import datetime
from fs_config import FSConfig
from wd_api import SearchAppApi


class WatchDogConfigTest(unittest.TestCase):
    def setUp(self):
        self.conf_file = 'fswatch.conf'
        self.config = FSConfig(self.conf_file)

    def testdbuser(self):
        self.assertEqual(self.config.dbuser(), 'watchdog')

    def testdbname(self):
        self.assertEqual(self.config.dbname(), 'watchdog')

    def testdbhost(self):
        self.assertEqual(self.config.dbhost(), "localhost")

    def testdbport(self):
        self.assertEqual(self.config.dbport(), '5432')

    def testappkey(self):
        self.assertEqual(self.config.search_key(), "my-search-key")

    def testsearchkey(self):
        self.assertEqual(self.config.app_key(), "my-app-key")


class WatchDogAPITest(unittest.TestCase):
    def setUp(self):
        self.conf_file = './fswatch.conf'
        self.config = FSConfig(self.conf_file)
        self.app_key = self.config.app_key()
        self.search_key = self.config.search_key()
        self.baseurl = self.config.base_url()
        self.api = SearchAppApi(self.search_key, self.app_key, self.baseurl)

    @patch('wd_api.SearchAppApi.init_schema')
    def test_mapping(self, *args):
        self.api.init_schema()
        self.api.init_schema.assert_called_once_with()

    @patch('wd_api.SearchAppApi.do_post')
    def test_post(self, *args):
        fname = './testfile.pdf'
        mimetype = "application/pdf"
        modified = datetime.datetime.now()
        owner = "test_user"
        perms = "06444"
        size = 1235
        self.api.do_post(
            fname,
            mimetype,
            owner,
            modified,
            perms,
            size
        )
        self.api.do_post.assert_called_once_with(
            fname, mimetype,
            owner, modified,
            perms, size
        )


if __name__ == '__main__':
    unittest.main()
