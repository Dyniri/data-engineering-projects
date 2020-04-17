from sqlalchemy import Column, ForeignKey, Integer, String, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()

class Event(Base):
    __tablename__ = 'events_target'
    event_id = Column(Integer, primary_key=True)
    artist = Column(String(94))
    firstName = Column(String(32))
    lastName = Column(String(32))
    gender = Column(String(1))
    length = Column(Integer)
    level = Column(String(32))
    location = Column(String(64))
    sessionId = Column(Integer)
    song = Column(String(256))
    ts = Column(TIMESTAMP())
    userAgent = Column(String(256))
    userId = Column(Integer, nullable=True)

class Song(Base):
    __tablename__ = 'songs_target'
    song_id = Column(String(18), primary_key=True)
    title = Column(String(52))
    duration = Column(Integer)
    year = Column(Integer)
    artist_id = Column(String(18))
    artist_latitude = Column(Integer)
    artist_longitude = Column(Integer)
    artist_location = Column(String(32))
    artist_name = Column(String(94))

engine = create_engine('postgresql://postgres:superpassword@localhost:5432/postgres')

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)