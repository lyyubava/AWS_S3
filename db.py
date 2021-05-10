from pymysql import cursors, connect, Error
from config import Configuration


class DB:
    def __init__(self):
        self.connection = connect(host=Configuration.host,
                                  user=Configuration.user,
                                  password=Configuration.password,
                                  db=Configuration.db)
        self.cur = self.connection.cursor()

    def create_db(self):
        try:
            create_db_query = "CREATE DATABASE IF NOT EXISTS arrr"
            self.cur.execute(create_db_query)
        except Error as e:
            print(e)

    def create_table_apps(self):
        try:
            create_table_apps_query = "CREATE TABLE IF NOT EXISTS apps( name VARCHAR(50), " \
                                      "genre VARCHAR(10)," \
                                      " rating FLOAT, " \
                                      "version VARCHAR(10)," \
                                      "size_bytes INT, " \
                                      "is_awesome BOOL)"
            self.cur.execute(create_table_apps_query)
        except Error as e:
            print(e)

    def create_table_songs(self):
        create_table_songs_query = " CREATE TABLE IF NOT EXISTS songs (artist_name VARCHAR(50)," \
                                   "title VARCHAR(50)," \
                                   "year INT," \
                                   "release_ VARCHAR(50)," \
                                   "ingestion_time DATETIME)"
        self.cur.execute(create_table_songs_query)

    def create_table_movies(self):
        que = "DROP TABLE IF EXISTS movies"
        self.cur.execute(que)
        create_table_movies_query = "CREATE TABLE IF NOT EXISTS movies(original_title VARCHAR(50)," \
                                   "original_language VARCHAR(50)," \
                                   "budget BIGINT,is_adult BOOL,release_data DATE, " \
                                   "original_title_normalized VARCHAR(100))"
        self.cur.execute(create_table_movies_query)

    def insert_data_into_movies(self, *data):
        sql = "INSERT INTO `movies` (`original_title`, `original_language`, `budget`, `is_adult`,`release_data`, `original_title_normalized`) " \
              "VALUES (%s, %s, %s, %s, %s, %s)"
        self.cur.execute(sql, *data)
        self.connection.commit()


if __name__ == '__main__':
    db = DB()
