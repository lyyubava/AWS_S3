import config
import os
import json
from db import DB
import pymysql
import re

db = DB()


def normalize(s: str) -> str:
    space = r'\s'
    norm = re.sub(space, '_', s)
    print(norm)
    norm = norm.lower()
    pattern = r'[^_a-zA-Z]'
    norm = re.sub(pattern, '', norm)

    return norm


def insert_data_into_tables():
    for filename in os.listdir(config.Configuration.directory):

        with open(os.path.join(config.Configuration.directory, filename), 'r') as f:

            data = json.load(f)

            for key in data:

                if key['type'] == 'movie':
                    try:
                        original_title = key['data'].get('original_title')
                        if not original_title:
                            continue
                        original_language = key['data'].get('original_language', None)
                        budget = key['data'].get('budget', None)
                        is_adult = key['data'].get('budget', None)
                        release_date = key['data'].get('release_date', None)
                        original_title_normalized = normalize(key['data']['original_title'])
                        val = (original_title, original_language, budget, is_adult, release_date, original_title_normalized)

                        db.insert_data_into_movies(val)

                    except KeyError:
                        continue

                    except pymysql.err.DataError:
                        continue



insert_data_into_tables()