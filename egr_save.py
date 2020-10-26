import json

import psycopg2
from configparser import ConfigParser


class EgrSave:
    TABLE_NAMES = {'getAllVEDByRegNum': 'all_ved_by_reg_num',
                   'getAllAddressByRegNum': 'all_address_by_reg_num',
                   'getBaseInfoByRegNum': 'base_info_by_reg_num',
                   'getEventsByRegNum': 'events_by_reg_num',
                   'getShortInfoByRegNum': 'short_info_by_reg_num',
                   'getAllIPFIOByRegNum': 'all_ipfio_by_reg_num',
                   'getAllJurNamesByRegNum': 'all_jur_names_by_reg_num'}
    
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
        for reg_num, reg_num_info in jsons.items():
            for method, method_info in reg_num_info.items():
                for dict_info in method_info:
                    fields = 'reg_num, ' + str(list(dict_info.keys())).replace('\'', '')[1:-1]
                    values = [reg_num, ]
                    for value in list(dict_info.values()):
                        if type(value) == dict:
                            values.append(json.dumps(value, ensure_ascii=False))
                        else:
                            values.append(value)
                    query = f'INSERT INTO {self.TABLE_NAMES[method]}({fields}) VALUES({("%s, " * len(values))[:-2]});'
                    self.cur.execute(query, tuple(values))

    def clear_db(self, tables=None):
        if type(tables) == str:
            tables = [tables, ]
        elif not tables:
            tables = list(self.TABLE_NAMES.values())
        for table_name in tables:
            query = f'''TRUNCATE TABLE {table_name}
                        RESTART IDENTITY;'''
            self.cur.execute(query)

    def __del__(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()
