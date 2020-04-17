from extract import get_events
from load import connect, load_data

def main():
    events_path = './data/event_data/'

    events = get_events(events_path)

    connect()

    load_data(events)

if __name__ == "__main__":
    main()
