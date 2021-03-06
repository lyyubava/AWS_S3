import config
import os
import json
from db import Movies, Songs, Apps
import pymysql
from extract_files import extraction


def insert_data_into_tables(*tables):
    for filename in os.listdir(config.Configuration.directory):
        with open(os.path.join(config.Configuration.directory, filename), 'r') as f:
            data = json.load(f)
            for key in data:
                for table in tables:
                    if key['type'] == table.type:
                        try:
                            table.insert_data(key)
                        except pymysql.err.DataError:
                            continue


if __name__ == '__main__':
    extraction()
    table_songs = Songs()
    table_apps = Apps()
    table_movies = Movies()
    table_movies.create_table_movies()
    table_songs.create_table()
    table_apps.create_table()
    insert_data_into_tables(table_apps, table_songs, table_movies)