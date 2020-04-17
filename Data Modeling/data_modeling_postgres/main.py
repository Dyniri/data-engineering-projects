from etl import load_events, load_songs
from sql_queries import insert_songs, insert_events, test

def main():
    events_path = './data/event_data/'
    songs_path = './data/song_data/'

    events = load_events(events_path)
    songs = load_songs(songs_path)

    insert_events(events)
    insert_songs(songs)

    test()

if __name__ == "__main__":
    main()