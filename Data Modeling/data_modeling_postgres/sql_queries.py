from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.sql import select

from datetime import datetime

from sql_classes import Base, Event, Song

engine = create_engine('postgresql://postgres:superpassword@localhost:5432/postgres')

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)

session = DBSession()

def insert_songs(data):
    for song in data:
        new_song = Song()
        
        new_song.song_id = song['song_id']
        new_song.title = song['title']
        new_song.duration = song['duration']
        new_song.year = song['year']
        new_song.artist_id = song['artist_id']
        new_song.artist_latitude = song['artist_latitude']
        new_song.artist_longitude = song['artist_longitude']
        new_song.artist_location = song['artist_location']
        new_song.artist_name = song['artist_name']
        
        session.add(new_song)
    session.commit()


def insert_events(data):
    i = 0
    for event in data:
        new_event = Event()

        new_event.event_id = i
        new_event.artist = event['artist']
        new_event.firstName = event['firstName']
        new_event.lastName = event['lastName']
        new_event.gender = event['gender']
        new_event.length  = event['length']
        new_event.level = event['level']
        new_event.location = event['location']
        new_event.sessionId = event['sessionId']
        new_event.song = event['song']
        new_event.ts  = datetime.fromtimestamp(event['ts']/1000)
        new_event.userAgent = event['userAgent']

        if not event['userId']:
            new_event.userId = 0
        else:
            new_event.userId  = event['userId']

        session.add(new_event)
        i += 1
    
    session.commit()

def test():
    query = """
    SELECT events.ts AS start_time, events."userId" AS user_id, events.level, songs.song_id, songs.artist_id, events."sessionId" AS session_id,
	events.location AS location, events."userAgent" AS user_agent
    FROM events_target AS events
    JOIN songs_target AS songs ON events.song = songs.title
    """
    
    result = engine.execute(query)
    for row in result:
        print(row)