import configparser
import mimetypes
import logging

logger = logging.getLogger(__name__)


class FSConfig:
    """
    uses ConfigParser
    read :configfile
    sections: dglass, dwatch, file-types
    getting the config file is
    sort of a bootstrapping issue, right?
    """
    def __init__(self, configfile=None):
        self.config = configparser.ConfigParser()
        if configfile:
            self.config.readfp(open(configfile))
        else:
            self.config.readfp(open('fswatch.conf'))
        self._get_mimetypes()

    def dbname(self):
        return self.config.get("fswatch", "dbname")

    def dbuser(self):
        return self.config.get("fswatch", "dbuser")

    def dbpass(self):
        return self.config.get("fswatch", "dbpass")

    def dbport(self):
        return self.config.get("fswatch", "dbport")

    def dbhost(self):
        return self.config.get("fswatch", "dbhost")

    def app_key(self):
        return self.config.get("searchapp", "app_key")

    def search_key(self):
        return self.config.get("searchapp", "search_key")

    def schema(self):
        return self.config.get("searchapp", "schema")

    def base_url(self):
        return self.config.get("searchapp", "base_url")

    def filemagic(self):
        pass

    def watch_dir(self):
        try:
            watch_dir = self.config.get("fswatch", "watch_dir")
        except ConfigParser.NoOptionError as e:
            logger.error(e)
            return None
        return watch_dir

    def _get_mimetypes(self):
        ftypes = {}
        mimetypes.init()
        for ext in self.config.items("file-types")[0][1].split(','):
            try:
                ftypes[ext] = mimetypes.types_map['.' + ext]
            except KeyError as e:
                logger.critical("There is no MIME type for extension {0}".format(ext))
                ftypes[ext] = None
        self.mimetypes = ftypes
