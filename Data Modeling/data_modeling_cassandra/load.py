from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

session = None

def connect():
    """
    Connects to the Cassandra cluster and creates the database
    """

    cluster = Cluster(['127.0.0.1'])
    try:
        global session
        session = cluster.connect()
        print('Connected')
    except Exception as e:
        print(f'An error has occured: {e}')

    try:
        session.execute("CREATE KEYSPACE IF NOT EXISTS sparkify WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

        session.set_keyspace('sparkify')

        # Drop existing tables
        session.execute("DROP TABLE IF EXISTS session_item")
        session.execute("DROP TABLE IF EXISTS user_session")
        session.execute("DROP TABLE IF EXISTS user_song")

        print('Keyspace ready')
    except Exception as e:
        print(f'An error has occured: {e}')

def load_data(data):
    """
    Loads each table
    """

    print('----------------------------------------')
    print('Loading session_item')
    song_by_session(data)
    print('----------------------------------------')
    print('Loading user_session')
    song_by_user(data)
    print('----------------------------------------')
    print('Loading user_song')
    users_by_song(data)

def song_by_session(data): 
    create_table_query = """CREATE TABLE IF NOT EXISTS session_item(
        artist text,
        title text,
        length float,
        sessionID int,
        itemInSession int,
        PRIMARY KEY (sessionID, itemInSession)
    )"""

    try:
        session.execute(create_table_query)
        print('session_item table created.')
    except Exception as e:
        print(f'An error has occured: {e}')
        return

    # Create a prepared statement for batch inserts
    try:
        insert_query = session.prepare('INSERT INTO session_item (artist, title, length, sessionID, itemInSession) VALUES (?, ?, ?, ?, ?)')
    except Exception as e:
        print(f'An error has occured: {e}')

    batch = BatchStatement()
    batchCounter = 0

    # Create a batch statement, limiting the batch to 100 records - Cassandra has limits
    for record in data:

        if record[6] == '':
            record[6] = 0

        batch.add(insert_query, (record[0], record[13], float(record[6]), int(record[12]), int(record[4])))

        if batchCounter == 100:
            try:
                session.execute(batch)
            except Exception as e:
                print(f'An error has occured: {e}')
            
            batch.clear()
            batchCounter = 0
        else:
            batchCounter = batchCounter + 1

    
    # Testing
    rows = session.execute('SELECT artist, title, length FROM session_item where sessionID = 338 AND itemInSession = 4')

    print('----------------------------------------')
    print('Test query results:')
    for row in rows:
        print(row)

def song_by_user(data):
    create_table_query = """CREATE TABLE IF NOT EXISTS user_session(
        artist text,
        title text,
        firstName text,
        lastName text,
        userID int,
        itemInSession int,
        sessionID int,
        PRIMARY KEY ((sessionID, userID), itemInSession)) WITH CLUSTERING ORDER BY (itemInSession ASC)
    """

    try:
        session.execute(create_table_query)
        print('user_session table created.')
    except Exception as e:
        print(f'An error has occured: {e}')
        return

    # Create a prepared statement for batch inserts
    try:
        insert_query = session.prepare('INSERT INTO user_session (artist, title, firstName, lastName, userID, itemInSession, sessionID) VALUES (?, ?, ?, ?, ?,?,?)')
    except Exception as e:
        print(f'An error has occured: {e}')

    batch = BatchStatement()
    batchCounter = 0

    # Create a batch statement, limiting the batch to 100 records - Cassandra has limits
    for record in data:

        if record[16] == '':
            record[16] = 0
 
        batch.add(insert_query, (record[0], record[13], record[2], record[5], int(record[16]), int(record[4]), int(record[12])))
        
        if batchCounter == 100:
            try:
                session.execute(batch)
            except Exception as e:
                print(f'An error has occured: {e}')
            
            batch.clear()
            batchCounter = 0
        else:
            batchCounter = batchCounter + 1

    
    # Testing
    rows = session.execute('SELECT artist, title, firstName, lastName FROM user_session where sessionID = 182 AND userID = 10')

    print('----------------------------------------')
    print('Test query results:')
    for row in rows:
        print(row)

def users_by_song(data):
    create_table_query = """CREATE TABLE IF NOT EXISTS user_song(
        title text,
        firstName text,
        lastName text,
        userID int,
        PRIMARY KEY ((title), userID)
        )
    """

    try:
        session.execute(create_table_query)
        print('user_song table created.')
    except Exception as e:
        print(f'An error has occured: {e}')
        return

    # Create a prepared statement for batch inserts
    try:
        insert_query = session.prepare('INSERT INTO user_song (title, firstName, lastName, userID) VALUES (?, ?, ?, ?)')
    except Exception as e:
        print(f'An error has occured: {e}')

    batch = BatchStatement()
    batchCounter = 0

    # Create a batch statement, limiting the batch to 100 records - Cassandra has limits
    for record in data:
        if record[13] != '':
            batch.add(insert_query, (record[13], record[2], record[5], int(record[16])))

            if batchCounter == 100:
                try:
                    session.execute(batch)
                except Exception as e:
                    print(f'An error has occured: {e}')

                batch.clear()
                batchCounter = 0
            else:
                batchCounter = batchCounter + 1

    
    # Testing
    rows = session.execute("SELECT title, firstName, lastName FROM user_song where title = 'All Hands Against His Own'")

    print('----------------------------------------')
    print('Test query results:')
    for row in rows:
        print(row)