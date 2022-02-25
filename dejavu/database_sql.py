# Database operations from the PyDejavu package
# Added in custom database operations for calculating
# confidence and working with forum data

from __future__ import absolute_import
from itertools import izip_longest
import Queue

import MySQLdb as mysql
from MySQLdb.cursors import DictCursor

from dejavu.database import Database


class SQLDatabase(Database):
    """
    Queries:

    1) Find duplicates (shouldn't be any, though):

        select `hash`, `song_id`, `offset`, count(*) cnt
        from fingerprints
        group by `hash`, `song_id`, `offset`
        having cnt > 1
        order by cnt asc;

    2) Get number of hashes by song:

        select song_id, song_name, count(song_id) as num
        from fingerprints
        natural join songs
        group by song_id
        order by count(song_id) desc;

    3) get hashes with highest number of collisions

        select
            hash,
            count(distinct song_id) as n
        from fingerprints
        group by `hash`
        order by n DESC;

    => 26 different songs with same fingerprint (392 times):

        select songs.song_name, fingerprints.offset
        from fingerprints natural join songs
        group by song_id
        order by count(song_id) desc;

    3) get hashes with highest number of collisions

        select
            hash,
            count(distinct song_id) as n
        from fingerprints
        group by `hash`
        order by n DESC;

    => 26 different songs with same fingerprint (392 times):

        select songs.song_name, fingerprints.offset
        from fingerprints natural join songs
        where fingerprints.hash = "08d3c833b71c60a7b620322ac0c0aba7bf5a3e73";
    """

    type = "mysql"

    # tables
    FINGERPRINTS_TABLENAME = "fingerprints"
    SONGS_TABLENAME = "songs"
    MATCH_DATA_TABLENAME = "match_data"
    FORUM_POSTS_TABLENAME = "forum_posts"
    COMMENTS_TABLENAME = "comments"
    POTENTIAL_MATCH_TABLENAME = "potential_matches"

    # fields
    FIELD_HASH = "hash"
    FIELD_SONG_ID = "song_id"
    FIELD_OFFSET = "offset"
    FIELD_SONGNAME = "song_name"
    FIELD_FINGERPRINTED = "fingerprinted"
    FIELD_MATCH_ENTRY_ID = "matchEntryID"
    FIELD_MATCHID = "matchID"
    FIELD_UID = "userID"
    FIELD_USER = "userName"
    FIELD_LATITUDE = "latitude"
    FIELD_LONGITUDE = "longitude"
    FIELD_MATCH_NAME = "matchName"
    FIELD_CONFIDENCE_LEVEL = "confidenceLevel"
    FIELD_POSTID = "postID"
    FIELD_COMMENTID = "commentID"
    FIELD_CATEGORY = "category"
    FIELD_TITLE = "title"
    FIELD_AUTHOR = "author"
    FIELD_CONTENT = "content"
    FIELD_DATE = "date"
    FIELD_IMAGE_FILENAME = "imageFilename"
    FIELD_AUDIO_FILENAME = "audioFilename"
    FIELD_SCIENTIFIC_NAME = "scientificName"
    FIELD_COMMON_NAME = "commonName"
    FIELD_HOST_SPECIES = "hostSpecies"
    FIELD_HOST_STATUS = "hostStatus"

    # creates
    CREATE_FINGERPRINTS_TABLE = """
        CREATE TABLE IF NOT EXISTS `%s` (
             `%s` binary(10) not null,
             `%s` mediumint unsigned not null,
             `%s` int unsigned not null,
         INDEX (%s),
         UNIQUE KEY `unique_constraint` (%s, %s, %s),
         FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE CASCADE
    ) ENGINE=INNODB;""" % (
        FINGERPRINTS_TABLENAME, FIELD_HASH,
        FIELD_SONG_ID, FIELD_OFFSET, FIELD_HASH,
        FIELD_SONG_ID, FIELD_OFFSET, FIELD_HASH,
        FIELD_SONG_ID, SONGS_TABLENAME, FIELD_SONG_ID
    )

    CREATE_SONGS_TABLE = """
        CREATE TABLE IF NOT EXISTS `%s` (
            `%s` mediumint unsigned not null auto_increment,
            `%s` varchar(250) not null,
            `%s` tinyint default 0,
        PRIMARY KEY (`%s`),
        UNIQUE KEY `%s` (`%s`)
    ) ENGINE=INNODB;""" % (
        SONGS_TABLENAME, FIELD_SONG_ID, FIELD_SONGNAME, FIELD_FINGERPRINTED,
        FIELD_SONG_ID, FIELD_SONG_ID, FIELD_SONG_ID,
    )

    CREATE_MATCH_DATA_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s INT not null,
            %s INT not null,
            %s VARCHAR(250) not null,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
        PRIMARY KEY (%s)
    );""" % (
        MATCH_DATA_TABLENAME, FIELD_MATCH_ENTRY_ID,
        FIELD_MATCHID, FIELD_UID, FIELD_USER,
        FIELD_LATITUDE, FIELD_LONGITUDE,
        FIELD_MATCH_NAME, FIELD_CONFIDENCE_LEVEL,
        FIELD_MATCH_ENTRY_ID,
    )

    CREATE_FORUM_POSTS_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s VARCHAR(250) not null,
            %s INT NOT NULL,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
        PRIMARY KEY (%s)
    );""" % (
        FORUM_POSTS_TABLENAME, FIELD_UID,
        FIELD_POSTID, FIELD_CATEGORY,
        FIELD_TITLE, FIELD_AUTHOR,
        FIELD_CONTENT, FIELD_DATE,
        FIELD_IMAGE_FILENAME, FIELD_AUDIO_FILENAME,
        FIELD_POSTID
    )

    CREATE_COMMENTS_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s VARCHAR(250) not null,
            %s INT,
            %s INT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            FOREIGN KEY (%s) REFERENCES %s (%s)

    );""" % (
        COMMENTS_TABLENAME, FIELD_UID,
        FIELD_POSTID, FIELD_COMMENTID,
        FIELD_CATEGORY, FIELD_AUTHOR,
        FIELD_CONTENT, FIELD_DATE,
        FIELD_POSTID, FORUM_POSTS_TABLENAME,
        FIELD_POSTID
    )

    CREATE_POTENTIAL_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS %s (
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT,
            %s TEXT
        );
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_SCIENTIFIC_NAME, FIELD_COMMON_NAME,
           FIELD_HOST_SPECIES, FIELD_HOST_STATUS,
           FIELD_REGION, FIELD_DESCRIPTION)

    # inserts (ignores duplicates)
    INSERT_FINGERPRINT = """
        INSERT IGNORE INTO %s (%s, %s, %s) values
            (UNHEX(%%s), %%s, %%s);
    """ % (FINGERPRINTS_TABLENAME, FIELD_HASH, FIELD_SONG_ID, FIELD_OFFSET)

    INSERT_MATCH = """
        INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) values
            (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s);
    """ % (MATCH_DATA_TABLENAME, FIELD_MATCH_ENTRY_ID, FIELD_MATCHID,
           FIELD_UID, FIELD_USER, FIELD_LATITUDE, FIELD_LONGITUDE,
           FIELD_MATCH_NAME, FIELD_CONFIDENCE_LEVEL)

    INSERT_SONG = "INSERT INTO %s (%s) values (%%s);" % (
        SONGS_TABLENAME, FIELD_SONGNAME)

    INSERT_POST = """
        INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s) values
            (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s);
    """ % (FORUM_POSTS_TABLENAME, FIELD_UID, FIELD_POSTID,
           FIELD_CATEGORY, FIELD_TITLE, FIELD_AUTHOR,
           FIELD_CONTENT, FIELD_DATE, FIELD_IMAGE_FILENAME,
           FIELD_AUDIO_FILENAME)

    INSERT_COMMENT = """
        INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) values
            (%%s, %%s, %%s, %%s, %%s, %%s, %%s);
    """ % (COMMENTS_TABLENAME, FIELD_UID, FIELD_POSTID, FIELD_COMMENTID,
           FIELD_CATEGORY, FIELD_AUTHOR, FIELD_CONTENT, FIELD_DATE)

    INSERT_POTENTIAL_MATCH = """
        INSERT INTO %s (%s, %s, %s, %s, %s, %s) values
            (%%s, %%s, %%s, %%s, %%s, %%s);
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_SCIENTIFIC_NAME, FIELD_COMMON_NAME,
           FIELD_HOST_SPECIES, FIELD_HOST_STATUS,
           FIELD_REGION, FIELD_DESCRIPTION)

    # selects
    SELECT = """
        SELECT %s, %s FROM %s WHERE %s = UNHEX(%%s);
    """ % (FIELD_SONG_ID, FIELD_OFFSET, FINGERPRINTS_TABLENAME, FIELD_HASH)

    SELECT_MULTIPLE = """
        SELECT HEX(%s), %s, %s FROM %s WHERE %s IN (%%s);
    """ % (FIELD_HASH, FIELD_SONG_ID, FIELD_OFFSET,
           FINGERPRINTS_TABLENAME, FIELD_HASH)

    SELECT_ALL = """
        SELECT %s, %s FROM %s;
    """ % (FIELD_SONG_ID, FIELD_OFFSET, FINGERPRINTS_TABLENAME)

    SELECT_SONG = """
        SELECT %s FROM %s WHERE %s = %%s
    """ % (FIELD_SONGNAME, SONGS_TABLENAME, FIELD_SONG_ID)

    SELECT_NUM_FINGERPRINTS = """
        SELECT COUNT(*) as n FROM %s
    """ % (FINGERPRINTS_TABLENAME)

    SELECT_INDIVIDUAL_FINGERPRINTS = """
        SELECT COUNT(*) as n FROM %s WHERE %s = %%s;
    """ % (FINGERPRINTS_TABLENAME, FIELD_SONG_ID)

    SELECT_UNIQUE_SONG_IDS = """
        SELECT COUNT(DISTINCT %s) as n FROM %s WHERE %s = 1;
    """ % (FIELD_SONG_ID, SONGS_TABLENAME, FIELD_FINGERPRINTED)

    SELECT_SONGS = """
        SELECT %s, %s FROM %s WHERE %s = 1;
    """ % (FIELD_SONG_ID, FIELD_SONGNAME, SONGS_TABLENAME, FIELD_FINGERPRINTED)

    SELECT_ALL_MATCH_DATA = """
        SELECT * FROM %s WHERE %s = %%s;
    """ % (MATCH_DATA_TABLENAME, FIELD_UID)

    SELECT_MATCH_DATA = """
        SELECT * FROM %s WHERE %s = %%s AND %s = %%s;
    """ % (MATCH_DATA_TABLENAME, FIELD_MATCHID, FIELD_UID)

    SELECT_NUM_MATCHES = """
        SELECT COUNT(*) as n from %s;
    """ % (MATCH_DATA_TABLENAME)

    SELECT_LAST_MATCH_ID = """
        SELECT %s FROM %s WHERE %s = %%s ORDER BY %s DESC LIMIT 1;
    """ % (FIELD_MATCHID, MATCH_DATA_TABLENAME, FIELD_UID, FIELD_MATCHID)

    SELECT_POSTS_BY_CATEGORY = """
        SELECT * FROM %s WHERE %s = %%s ORDER BY %s ASC;
    """ % (FORUM_POSTS_TABLENAME, FIELD_CATEGORY, FIELD_DATE)

    SELECT_NUM_POSTS = """
        SELECT COUNT(*) as n from %s
    """ % (FORUM_POSTS_TABLENAME)

    SELECT_USER_POSTS = """
        SELECT * FROM %s WHERE %s = %%s ORDER BY %s ASC;
    """ % (FORUM_POSTS_TABLENAME, FIELD_UID, FIELD_DATE)

    SELECT_NUM_USER_POSTS = """
        SELECT COUNT(*) as n from %s WHERE %s = %%s;
    """ % (FORUM_POSTS_TABLENAME, FIELD_UID)

    SELECT_SPECIFIC_POST = """
        SELECT * FROM %s WHERE %s = %%s AND %s = %%s;
    """ % (FORUM_POSTS_TABLENAME, FIELD_UID, FIELD_POSTID)

    SELECT_LAST_POST = """
        SELECT * FROM %s WHERE %s = (SELECT max(%s) FROM %s);
    """ % (FORUM_POSTS_TABLENAME, FIELD_POSTID,
           FIELD_POSTID, FORUM_POSTS_TABLENAME)

    SELECT_COMMENTS = """
        SELECT * FROM %s;
    """ % (COMMENTS_TABLENAME)

    SELECT_NUM_COMMENTS = """
        SELECT COUNT(*) as n from %s
    """ % (COMMENTS_TABLENAME)

    SELECT_SPECIFIC_COMMENTS = """
        SELECT * FROM %s WHERE %s = %%s AND %s = %%s;
    """ % (COMMENTS_TABLENAME, FIELD_POSTID, FIELD_CATEGORY)

    SELECT_ALL_POTENTIAL_MATCHES = """
        SELECT * FROM %s ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_HOST = """
        SELECT * FROM %s WHERE %s = %%s ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_HOST_SPECIES, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_REGION = """
        SELECT * FROM %s WHERE %s = %%s ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_REGION, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_STATUS = """
        SELECT * FROM %s WHERE %s LIKE concat(%%s, %%s, %%s) ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_HOST_STATUS, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_HOST_AND_REGION = """
        SELECT * FROM %s WHERE %s = %%s AND %s = %%s ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_HOST_SPECIES,
           FIELD_REGION, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_HOST_AND_STATUS = """
        SELECT * FROM %s WHERE %s = %%s AND
        %s LIKE concat(%%s, %%s, %%s) ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_HOST_SPECIES,
           FIELD_HOST_STATUS, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_STATUS_AND_REGION = """
        SELECT * FROM %s WHERE %s LIKE concat(%%s, %%s, %%s)
        AND %s = %%s ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_HOST_STATUS,
           FIELD_REGION, FIELD_COMMON_NAME)

    SELECT_POTENTIAL_MATCHES_BY_HOST_STATUS_AND_REGION = """
        SELECT * FROM %s WHERE %s = %%s AND %s LIKE
        concat(%%s, %%s, %%s) AND %s = %%s ORDER BY %s ASC;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_HOST_SPECIES,
           FIELD_HOST_STATUS, FIELD_REGION, FIELD_COMMON_NAME)

    # drops
    DROP_FINGERPRINTS = "DROP TABLE IF EXISTS %s;" % FINGERPRINTS_TABLENAME
    DROP_SONGS = "DROP TABLE IF EXISTS %s;" % SONGS_TABLENAME

    # updates
    UPDATE_SONG_FINGERPRINTED = """
        UPDATE %s SET %s = 1 WHERE %s = %%s
    """ % (SONGS_TABLENAME, FIELD_FINGERPRINTED, FIELD_SONG_ID)

    UPDATE_USERNAME_ON_POSTS = """
        UPDATE %s SET %s = %%s WHERE %s = %%s;
    """ % (FORUM_POSTS_TABLENAME, FIELD_AUTHOR, FIELD_UID)

    UPDATE_USERNAME_ON_COMMENTS = """
        UPDATE %s SET %s = %%s WHERE %s = %%s;
    """ % (COMMENTS_TABLENAME, FIELD_AUTHOR, FIELD_UID)

    # deletes
    DELETE_UNFINGERPRINTED = """
        DELETE FROM %s WHERE %s = 0;
    """ % (SONGS_TABLENAME, FIELD_FINGERPRINTED)

    DELETE_MATCH_DATA = """
        DELETE FROM %s WHERE %s = %%s;
    """ % (MATCH_DATA_TABLENAME, FIELD_MATCHID)

    DELETE_POST = """
        DELETE FROM %s WHERE %s = %%s AND %s = %%s;
    """ % (FORUM_POSTS_TABLENAME, FIELD_UID, FIELD_POSTID)

    DELETE_POST_COMMENTS = """
        DELETE FROM %s WHERE %s = %%s;
    """ % (COMMENTS_TABLENAME, FIELD_POSTID)

    DELETE_COMMENT = """
        DELETE FROM %s WHERE %s = %%s AND %s = %%s AND %s = %%s;
    """ % (COMMENTS_TABLENAME, FIELD_UID, FIELD_POSTID, FIELD_COMMENTID)

    DELETE_POTENTIAL_MATCH = """
        DELETE FROM %s WHERE %s = %%s;
    """ % (POTENTIAL_MATCH_TABLENAME, FIELD_SCIENTIFIC_NAME)

    DELETE_FINGERPRINTS = """
        DELETE FROM %s
    """ % (FINGERPRINTS_TABLENAME)

    DELETE_SONGS = """
        DELETE FROM %s
    """ % (SONGS_TABLENAME)

    def __init__(self, **options):
        super(SQLDatabase, self).__init__()
        self.cursor = cursor_factory(**options)
        self._options = options

    def after_fork(self):
        # Clear the cursor cache, we don't want any stale connections from
        # the previous process.
        Cursor.clear_cache()

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.

        This also removes all songs that have been added but have no
        fingerprints associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.CREATE_SONGS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            cur.execute(self.DELETE_UNFINGERPRINTED)
            cur.execute(self.CREATE_MATCH_DATA_TABLE)
            cur.execute(self.CREATE_FORUM_POSTS_TABLE)
            cur.execute(self.CREATE_COMMENTS_TABLE)
            cur.execute(self.CREATE_POTENTIAL_MATCH_TABLE)

    def empty(self):
        """
        Drops tables created by dejavu and then creates them again
        by calling `SQLDatabase.setup`.

        .. warning:
            This will result in a loss of data
        """
        with self.cursor() as cur:
            cur.execute(self.DROP_FINGERPRINTS)
            cur.execute(self.DROP_SONGS)

        self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Removes all songs that have no fingerprints associated with them.
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_UNFINGERPRINTED)

    def delete_match(self, mid):
        """
        Removes a match entry given match ID
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_MATCH_DATA, (mid))

    def delete_post(self, uid, pid):
        """
        Removes a post given a user ID and post ID
            Note: must delete all post comments first
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_POST_COMMENTS, (pid,))
            cur.execute(self.DELETE_POST, (uid, pid,))

    def delete_comment(self, uid, pid, cid):
        """
        Removes a comment given a user ID and comment ID
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_COMMENT, (uid, pid, cid,))

    def delete_potential_match(self, scientific_name):
        """
        Deletes a potential match from the database using the scientific name
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_POTENTIAL_MATCH, (scientific_name,))

    def delete_fingerprints():
        """
        Deletes all fingerprints
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_FINGERPRINTS)

    def delete_songs():
        """
        Deletes all songs
        """
        with self.cursor() as cur:
            cur.execute(self.DELETE_SONGS)

    def get_all_match_data(self, uid):
        """
        Returns data for all match data for a user
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_ALL_MATCH_DATA, (uid,))

            for row in cur:
                yield row

    def get_match_data(self, mid, uid):
        """
        Returns data for all matches
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_MATCH_DATA, (mid, uid))

            for row in cur:
                yield row

    def get_last_match_id(self, uid):
        """
        Gets the most recent match id for a user
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_LAST_MATCH_ID, (uid,))

            for row in cur:
                yield row

    def get_num_matches(self):
        """
        Returns the number of match entries in the database for a given user
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_MATCHES)

            for count, in cur:
                return count
            return 0

    def get_posts_by_category(self, category):
        """
        Returns all post information from the database
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POSTS_BY_CATEGORY, (category,))

            for row in cur:
                yield row

    def get_num_posts(self):
        """
        Returns the number of forum posts in the database
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_POSTS)

            for count, in cur:
                return count
            return 0

    def get_user_posts(self, uid):
        """
        Returns the posts for a specific user
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_USER_POSTS, (uid,))

            for row in cur:
                yield row

    def get_specific_post(self, uid, postID):
        """
        Returns a specific post with the given user and post ID
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_SPECIFIC_POST, (uid, postID,))

            for row in cur:
                yield row

    def get_last_post(self):
        """
        Returns the last post in the database
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_LAST_POST)

            for row in cur:
                yield row

    def get_num_user_posts(self, uid):
        """
        Returns the number of posts for a specific user
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_USER_POSTS, (uid,))

            for count, in cur:
                return count
            return 0

    def get_comments(self):
        """
        Returns all comment information from the database
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_COMMENTS)

            for row in cur:
                yield row

    def get_num_comments(self):
        """
        Returns the number of comments in the database
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_COMMENTS)

            for count, in cur:
                return count
            return 0

    def get_specific_comments(self, pid, category):
        """
        Returns the comments for a post with the given post ID and category
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_SPECIFIC_COMMENTS, (pid, category,))

            for row in cur:
                yield row

    def get_num_songs(self):
        """
        Returns number of songs the database has fingerprinted.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_UNIQUE_SONG_IDS)

            for count, in cur:
                return count
            return 0

    def get_num_fingerprints(self):
        """
        Returns number of fingerprints the database has fingerprinted.
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_NUM_FINGERPRINTS)

            for count, in cur:
                return count
            return 0

    def get_individual_fingerprints(self, sid):
        """
        Returns number of individual fingerprints for
        song with matching song id
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_INDIVIDUAL_FINGERPRINTS, (sid,))

            for count, in cur:
                return count
            return 0

    def get_all_potential_matches(self):
        """
        Returns all potential matches
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_ALL_POTENTIAL_MATCHES)

            for row in cur:
                yield row

    def get_potential_matches_by_host(self, host):
        """
        Returns all potential matches for a host species
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_HOST, (host,))

            for row in cur:
                yield row

    def get_potential_matches_by_region(self, region):
        """
        Returns all potential matches for a host species
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_REGION, (region,))

            for row in cur:
                yield row

    def get_potential_matches_by_status(self, status):
        """
        Returns all potential matches containing the host status
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_STATUS,
                        ('%', status, '%',))

            for row in cur:
                yield row

    def get_potential_matches_by_host_and_region(self, host, region):
        """
        Returns all potential matches containing the host and region
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_HOST_AND_REGION,
                        (host, region,))

            for row in cur:
                yield row

    def get_potential_matches_by_host_and_status(self, host, status):
        """
        Returns all potential matches containing the host and host status
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_HOST_AND_STATUS,
                        (host, '%', status, '%',))

            for row in cur:
                yield row

    def get_potential_matches_by_status_and_region(self, status, region):
        """
        Returns all potential matches containing the host status and region
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_STATUS_AND_REGION,
                        ('%', status, '%', region,))

            for row in cur:
                yield row

    def get_potential_matches_by_host_status_and_region(self, host,
                                                        status, region):
        """
        Returns all potential matches containing the host, status and region
        """
        with self.cursor() as cur:
            cur.execute(self.SELECT_POTENTIAL_MATCHES_BY_HOST_STATUS_AND_REGION,
                        (host, '%', status, '%', region,))

            for row in cur:
                yield row

    def set_song_fingerprinted(self, sid):
        """
        Set the fingerprinted flag to TRUE (1) once a song has been completely
        fingerprinted in the database.
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_SONG_FINGERPRINTED, (sid,))

    def get_songs(self):
        """
        Return songs that have the fingerprinted flag set TRUE (1).
        """
        with self.cursor(cursor_type=DictCursor) as cur:
            cur.execute(self.SELECT_SONGS)
            for row in cur:
                yield row

    def set_username(self, newUsername, uid):
        """
        Sets the username field to a new username in the posts
        and comments databases
        """
        with self.cursor() as cur:
            cur.execute(self.UPDATE_USERNAME_ON_POSTS, (newUsername, uid,))
            cur.execute(self.UPDATE_USERNAME_ON_COMMENTS, (newUsername, uid,))

    def get_song_by_id(self, sid):
        """
        Returns song by its ID.
        """
        with self.cursor(cursor_type=DictCursor) as cur:
            cur.execute(self.SELECT_SONG, (sid,))
            return cur.fetchone()

    def insert(self, hash, sid, offset):
        """
        Insert a (sha1, song_id, offset) row into database.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_FINGERPRINT, (hash, sid, offset))

    def insert_song(self, songname):
        """
        Inserts song in the database and returns the ID of the inserted record.
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_SONG, (songname,))
            return cur.lastrowid

    def query(self, hash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        # select all if no key
        query = self.SELECT_ALL if hash is None else self.SELECT

        with self.cursor() as cur:
            cur.execute(query)
            for sid, offset in cur:
                yield (sid, offset)

    def insert_match_data(self, meid, mid, uid, user, latitude,
                          longitude, match_name, confidence):
        """
        Inserts a new match entry into database
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_MATCH, (meid, mid, uid, user, latitude,
                                            longitude, match_name, confidence))

    def insert_post(self, uid, pid, category, title, author,
                    content, date, image_filename, audio_filename):
        """
        Inserts a new forum post row into database
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_POST, (uid, pid, category, title, author,
                                           content, date, image_filename,
                                           audio_filename))

    def insert_comment(self, uid, pid, cid, author, content, date, category):
        """
        Inserts a new comment into the database
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_COMMENT,
                        (uid, pid, cid, author, content, date, category))

    def insert_potential_match(self, scientific_name, common_name,
                               host_species, host_status, region, description):
        """
        Inserts a new potential match into the database
        """
        with self.cursor() as cur:
            cur.execute(self.INSERT_POTENTIAL_MATCH,
                        (scientific_name, common_name, host_species,
                         host_status, region, description))

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => song_id, offset
        values into the database.
        """
        values = []
        for hash, offset in hashes:
            values.append((hash, sid, offset))

        with self.cursor() as cur:
            for split_values in grouper(values, 1000):
                cur.executemany(self.INSERT_FINGERPRINT, split_values)

    def return_matches(self, hashes):
        """
        Return the (song_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        # Create a dictionary of hash => offset pairs for later lookups
        mapper = {}
        for hash, offset in hashes:
            mapper[hash.upper()] = offset

        # Get an iteratable of all the hashes we need
        values = mapper.keys()

        with self.cursor() as cur:
            for split_values in grouper(values, 1000):
                # Create our IN part of the query
                query = self.SELECT_MULTIPLE
                query = query % ', '.join(['UNHEX(%s)'] * len(split_values))

                cur.execute(query, split_values)

                for hash, sid, offset in cur:
                    # (sid, db_offset - song_sampled_offset)
                    yield (sid, offset - mapper[hash])

    def __getstate__(self):
        return (self._options,)

    def __setstate__(self, state):
        self._options, = state
        self.cursor = cursor_factory(**self._options)


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in izip_longest(fillvalue=fillvalue, *args))


def cursor_factory(**factory_options):
    def cursor(**options):
        options.update(factory_options)
        return Cursor(**options)
    return cursor


class Cursor(object):
    """
    Establishes a connection to the database and returns an open cursor.


    ```python
    # Use as context manager
    with Cursor() as cur:
        cur.execute(query)
    ```
    """
    _cache = Queue.Queue(maxsize=5)

    def __init__(self, cursor_type=mysql.cursors.Cursor, **options):
        super(Cursor, self).__init__()

        try:
            conn = self._cache.get_nowait()
        except Queue.Empty:
            conn = mysql.connect(**options)
        else:
            # Ping the connection before using it from the cache.
            conn.ping(True)

        self.conn = conn
        self.conn.autocommit(False)
        self.cursor_type = cursor_type

    @classmethod
    def clear_cache(cls):
        cls._cache = Queue.Queue(maxsize=5)

    def __enter__(self):
        self.cursor = self.conn.cursor(self.cursor_type)
        return self.cursor

    def __exit__(self, extype, exvalue, traceback):
        # if we had a MySQL related error we try to rollback the cursor.
        if extype is mysql.MySQLError:
            self.cursor.rollback()

        self.cursor.close()
        self.conn.commit()

        # Put it back on the queue
        try:
            self._cache.put_nowait(self.conn)
        except Queue.Full:
            self.conn.close()
