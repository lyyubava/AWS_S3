from pymysql import connect, Error
from config import Configuration
from datetime import datetime


class DB:
    conn = connect(host=Configuration.host,
                   user=Configuration.user,
                   password=Configuration.password)

    cur = conn.cursor()

    def create_db(self):
        try:
            create_db_query = f"CREATE DATABASE IF NOT EXISTS {Configuration.db}"
            self.cur.execute(create_db_query)
        except Error as e:
            print(e)
        self.connection = connect(host=Configuration.host,
                                  user=Configuration.user,
                                  password=Configuration.password,
                                  db=Configuration.db)
        return self.connection

    def __init__(self):
        self.connection = self.create_db()
        self.cur = self.connection.cursor()


class Apps(DB):
    def create_table(self):
        try:
            create_table_apps_query = f"CREATE TABLE IF NOT EXISTS apps.{Configuration.db}( name VARCHAR(50), " \
                                      "genre VARCHAR(10)," \
                                      "rating FLOAT, " \
                                      "version VARCHAR(10)," \
                                      "size_bytes INT, " \
                                      "is_awesome BOOL)"
            self.cur.execute(create_table_apps_query)
        except Error as e:
            print(e)

    def insert_data(self, *data):
        sql = "INSERT INTO `apps` (`name`, `genre`, `rating`, `version`, `size_bytes`, `is_awesome`) " \
              "VALUES (%s, %s, %s, %s, %s, %s)"
        self.cur.execute(sql, *data)
        self.connection.commit()


class Songs(DB):
    def create_table(self):
        create_table_songs_query = f"CREATE TABLE IF NOT EXISTS {Configuration.db}.songs(artist_name VARCHAR(50)," \
                                   "title VARCHAR(50)," \
                                   "year INT," \
                                   "release_ VARCHAR(50)," \
                                   "ingestion_time DATETIME)"
        self.cur.execute(create_table_songs_query)

    def insert_data(self, *data):
        sql = "INSERT INTO `songs` (`artist_name`, `title`, `year`, `release_`, `ingestion_time`) " \
              "VALUES (%s, %s, %s, %s, %s)"
        self.cur.execute(sql, *data)
        self.connection.commit()


class Movies(DB):
    def create_table_movies(self):
        create_table_movies_query = "CREATE TABLE IF NOT EXISTS movies(original_title VARCHAR(50)," \
                                    "original_language VARCHAR(50)," \
                                    "budget BIGINT,is_adult BOOL,release_date DATE, " \
                                    "original_title_normalized VARCHAR(100))"
        self.cur.execute(create_table_movies_query)

    def insert_data(self, *data):
        sql = "INSERT INTO `movies` (`original_title`, `original_language`, `budget`, " \
              "`is_adult`,`release_date`, `original_title_normalized`) " \
              "VALUES (%s, %s, %s, %s, %s, %s)"
        self.cur.execute(sql, *data)
        self.connection.commit()


if __name__ == '__main__':
    db = DB()
    db.create_db()
    song_table = Songs()
    song_table.create_table()
    song_table.insert_data(("Massive Attack", "Karmacoma", 1994, "Protection", datetime.now()))
