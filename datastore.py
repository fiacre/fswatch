from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from config import Config

config = Config()

Base = declarative_base()


class FileData(Base):
    """
    owner,
    group,
    filesize,
    md5,
    mimetype,
    fileperm,
    mtime
    """
    __tablename__ = 'filedata'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    md5_hash = Column(String)
    owner = Column(String)
    group = Column(String)
    filesize = Column(Integer)
    permissions = Column(String)
    mtime = Column(DateTime)

    def __init__(self, name, md5_hash, owner, group, filesize, permissions, mtime):
        self.name = name
        self.md5_hash = md5_hash
        self.owner = owner
        self.group = group
        self.filesize = filesize
        self.permissions = permissions
        self.mtime = mtime

    def __repr__(self):
        return u"{0} {1}".format(self.name, self.owner, self.group, self.md5_hash)


engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(
        config.dbuser(),
        config.dbpass(),
        config.dbhost(),
        config.dbport(),
        config.dbname()
    )
)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

