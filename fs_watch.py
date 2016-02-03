#!  /usr/bin/env   python
import hashlib
import logging
import mimetypes
import os
import time
import datetime
from optparse import OptionParser
from os import stat
import sys
from stat import ST_MODE
from datastore import FileData, Session
from config import Config
from pwd import getpwuid
import grp

from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from wd_api import SearchAppApi

logger = logging.getLogger(__name__)


class WalkException(Exception):
    pass


class Watcher(LoggingEventHandler):
    """
    extend file system event handler:

    """
    def __init__(self, *args, **kwargs):
        super(Watcher, self).__init__(*args, **kwargs)
        self.session = Session()
        self.api = SearchAppApi(config.search_key(), config.app_key(), config.base_url(), config.schema())

    def fileinfo(self, path):
        """
        :param fname: filename
        :returns list of
            owner
            group
            file size
            md5
            mimetype
            file permissions
            mtime
        """
        owner = self._owner(path)
        md5 = self._file_hash(path)
        mtime = self._mtime(path)
        mimetype = self._file_mimetype(path)
        filesize = self._filesize(path)
        fileperm = self._fileperms(path)
        group = self._groupname(path)

        return owner, group, filesize, md5, mimetype, fileperm, mtime

    def on_created(self, event):
        """Called when a file or directory is created.
        :param event:
            Event representing file/directory creation.
        :type event:
            :class:`DirCreatedEvent` or :class:`FileCreatedEvent`
        """
        super(Watcher, self).on_created(event)
        # if directory is created, walk will take care of it
        if event.is_directory:
            pass
        else:
            try:
                # this may be a temporary file created by an app
                (owner, group, filesize, md5, mimetype, fileperm, mtime) = self.fileinfo(event.src_path)
            except OSError as e:
                logger.error(u"No such file: {0}: {1}".format(event.src_path, e))
            else:
                # if there are file stats, deal with it
                if mimetype:
                    file_obj = FileData(
                            name=event.src_path,
                            md5_hash=md5,
                            owner=owner,
                            group=group,
                            filesize=filesize,
                            permissions=fileperm,
                            mtime=mtime
                    )
                    self.session.add(file_obj)
                    self.session.commit()
                    self.api.do_post(event.src_path, mimetype, owner, mtime, fileperm, filesize)
                    print(u"Created: {0}, {1}, {2}, {3}, {4}, {5}".format(
                        event.src_path,
                        owner,
                        filesize,
                        md5,
                        mimetype,
                        fileperm)
                    )

    def on_deleted(self, event):
        """
        We ignore deletes (for now)
        just log
        """
        super(Watcher, self).on_deleted(event)

    def on_modified(self, event):
        """Called when a file or directory is modified.

        :param event:
            Event representing file/directory modification.
        :type event:
            :class:`DirModifiedEvent` or :class:`FileModifiedEvent`
        """
        super(Watcher, self).on_modified(event)
        if event.is_directory:
            pass
        else:
            (owner, group, filesize, md5, mimetype, fileperm, mtime) = self.fileinfo(event.src_path)
            file_data = self.session.query(FileData).filter(FileData.name==event.src_path, FileData.md5_hash!=md5)
            if file_data.count() > 0:
                session.query(FileData).update(
                        {
                            FileData.md5_hash: md5,
                            FileData.filesize: filesize,
                            FileData.owner: owner,
                            FileData.group: group,
                            FileData.mtime: mtime,
                            FileData.permissions: fileperm
                        })
                self.session.commit()
                self.api.do_post(event.src_path, mimetype, owner, mtime, fileperm, filesize)

    def _file_mimetype(self, fname):
        """
        :param full path to file
        :return mime type (if known) of the file
        """
        try:
            suffix = os.path.splitext(os.path.split(fname)[-1])[1]
        except IndexError as e:
            logger.error("{0} has no file extension".format(fname))
            return None
        else:
            mimetypes.init()
            return mimetypes.types_map.get(suffix) or None

    def _file_hash(self, fname):
        """
        :param full pathname of file:
        :return: md5 hexdigest of fname
        """
        return hashlib.md5(fname).hexdigest()

    def _owner(self, fname):
        """
        :param fname: full path to file
        :return: owner of file
        """
        return getpwuid(stat(fname).st_uid).pw_name

    def _fileperms(self, fname):
        """
        :param full pathname of file:
        :return: octal repr of file perms
        """
        return oct(os.stat(fname)[ST_MODE] & 0777)

    def _filesize(self, fname):
        """
        :param fname:
        :return: file size in byes
        """
        return os.stat(fname).st_size

    def _mtime(self, fname):
        """
        :param fname:
        :return: mod time as dateime obj
        """
        return datetime.datetime.fromtimestamp(os.stat(fname).st_mtime)

    def _groupname(self, fname):
        """
        :param fname:
        :return: get the group name of fname
        FIXME: groups in Windoze??

        """
        if sys.platform == 'win32':
            return None
        else:
            return grp.getgrgid(os.stat(fname).st_gid).gr_name

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-d', '--init-dir',
                      help="top director to watch",
                      action="store",
                      type="string",
                      dest='dir')
    parser.add_option('-c', '--conf-file',
                        action="store",
                        type="string",
                        help='location of conf file (if not .)',
                        dest='conf')
    (options, args) = parser.parse_args()

    conf_file = './fswatch.conf'
    if options.conf:
        conf_file = options.conf

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    config = Config(conf_file)
    watch_dir = ''
    if config.watch_dir() is not None:
        watch_dir = config.watch_dir()
    session = Session()
    event_handler = Watcher()
    api = SearchAppApi(config.search_key(), config.app_key(), config.base_url(), config.schema())

    if options.dir:
        watch_dir = options.dir

    def manage_walkexception():
        pass

    if watch_dir == '' or watch_dir is None:
        raise WalkException("Set a directory to watch either from command line for config file")
    for root, dirs, files in os.walk(watch_dir, topdown=True, onerror=manage_walkexception(),followlinks=False):
        for fname in files:
            (owner, group, filesize, md5, mimetype, fileperm, mtime) = event_handler.fileinfo(os.path.join(root, fname))
            if mimetype in config.mimetypes.values():
                fd = FileData(
                    name=os.path.join(root, fname),
                    md5_hash=md5,
                    owner=owner,
                    group=group,
                    filesize=filesize,
                    permissions=fileperm,
                    mtime=mtime
                )
                # not seen tis file before
                if session.query(FileData).filter(FileData.name==fd.name, fd.md5_hash==md5).count() == 0:
                    session.add(fd)
                    session.commit()
                    api.do_post(os.path.join(root, fname), mimetype, owner, mtime, fileperm, filesize)
                else:
                    # log it
                    logger.info("File: {fname} in {path}, was already posted to {schema}".format(
                        fname=fname, path=root, schema=config.schema()
                    ))
    observer = Observer()
    try:
        observer.schedule(event_handler, watch_dir, recursive=True)
        observer.start()
    except IOError as e:
        logger.error("Caught IOError: {}".format(e))
        # print("caught IOError: {}".format(e))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
