# Queries from project requirements

# Query 1:
# Since the data is uniquely identified by the combination of sessionId and itemInSession, these two columns form the Composite Primary Key of the table.  
# We want the data distributed as evenly as possible, the better choice for the Primary key is sessionId since itemInSession is expected to be skewed towards 0

create_song_play_session_items = """
CREATE TABLE IF NOT EXISTS song_play_session_items (
sessionId int, itemInSession int, artist text, song text, length decimal,
PRIMARY KEY (sessionId, itemInSession)
)
"""

query1 = """ 
SELECT artist, song as songTitle, length as songLength
FROM song_play_session_items
WHERE sessionId=338 AND itemInSession=4
"""


# Query 2:
# The same logic applies to this query as in Query 1.  
# We use the sessionId as the Primary Key and userId and itemInSession as the clustering columns
# The query will first use userId and then sessionId in its where clause.  
# Finally, it will be ordered by itemInSession which means ordering by the other two columns too

create_artist_song_session_plays = """
CREATE TABLE IF NOT EXISTS artist_song_session_plays (
sessionId int, userId int, itemInSession int, artist text, song text, firstName text, lastName text,
PRIMARY KEY (sessionId, userId, itemInSession)
)
"""

query2 = """ 
SELECT artist, song as songTitle, firstName, lastName
FROM artist_song_session_plays
WHERE userId=10 AND sessionId=182
ORDER BY userId, itemInSession 
"""


# Query 3:
# This query concerns which users listen to an artist
# Since a two users can share the same name, we need to use their userId as the unique identifier along with song.  Even though the userId is not required in the actual query.
# We therefore have song as the primary key and userId as the clustering column

create_user_song_plays = """
CREATE TABLE IF NOT EXISTS user_song_plays (
song text, userId int, firstName text, lastName text, 
PRIMARY KEY (song, userId)
)
"""

query3 = """ 
SELECT firstName, lastName
FROM user_song_plays
WHERE song='All Hands Against His Own'
"""
