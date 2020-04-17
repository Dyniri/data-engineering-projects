-- Dimension Tables --

--Users table
SELECT DISTINCT "userId" as user_id, "firstName" as first_name, "lastName" as last_name, gender, level FROM events_target
where "userId" != 0
order by user_id

-- Songs table
SELECT DISTINCT song_id, title, artist_id, year, duration FROM songs_target;

-- Artist table
SELECT DISTINCT artist_id, artist_name as name, 
artist_location as location, artist_latitude as latitude, artist_longitude as longitude 
FROM songs_target;

-- Time table
SELECT DISTINCT ts as start_time, extract(HOUR FROM ts) as hour, extract(DAY FROM ts) as day, extract(WEEK FROM ts) as week,
extract(MONTH FROM ts) as month, extract(YEAR FROM ts) as year, extract(DOW FROM ts) as weekday FROM events_target

-- Fact Table
-- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
SELECT events.ts AS start_time, events."userId" AS user_id, events.level, songs.song_id, songs.artist_id, events."sessionId" AS session_id,
	events.location AS location, events."userAgent" AS user_agent
FROM events_target AS events
JOIN songs_target AS songs ON events.song = songs.title