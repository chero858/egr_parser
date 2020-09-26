import json

import psycopg2
from configparser import ConfigParser


class EgrSave:
    def __init__(self):
        self.db_params = {}
        self.conn = None
        self.cur = None
        self.connect()

    def connect(self):
        try:
            self.config()
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**self.db_params)
            self.cur = self.conn.cursor()
            self.cur.execute('SELECT version()')
            db_version = self.cur.fetchone()
            print('PostgreSQL database version:', db_version)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def config(self, filename='database.ini', section='postgresql'):
        parser = ConfigParser()
        parser.read(filename)
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                self.db_params[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    def save_data(self, jsons):
        for regnum, regnum_info in jsons.items():
            for method, info in regnum_info.items():
                query = f"INSERT INTO regnums_info(regnum, method, info) VALUES({regnum}, '{method}', '{json.dumps(info, ensure_ascii=False)}') "
                self.cur.execute(query)

    def clear_db(self):
        query = f'''TRUNCATE TABLE regnums_info 
                    RESTART IDENTITY;'''
        self.cur.execute(query)

    def __del__(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()
