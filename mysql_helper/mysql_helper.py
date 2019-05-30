import pymysql
import json
import pandas as pd

class MysqlHelper:
    def __init__(self, config_path="mysql_helper_config.json", config_version="default"):
        with open("mysql_helper_config.json", "r") as config_file:
            self.config = json.load(config_file)
        self.config_version = config_file
        self.connection = self._get_connection(config_version)
    def __del__(self):
        self.close_connection()
    def generator_query(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            while result is not None:
                yield result
                result = cursor.fetchone()
    def query(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        return result
    def commit_query(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
        self.connection.commit()
    def dataframe_query(self, sql):
        return pd.DataFrame(self.query(sql))
    def list2str(self, query_list):
        return '(' + ''.join(['"' + i + '",' for i in query_list[:-1]]) + '"' + query_list[-1] + '")' 
    def close_connection(self):
        self.connection.close()
        print("connection closed")
    def change_version(self, config_version):
        self.close_connection()
        self.config_version = config_version
        self.connect()
    def connect(self):
        self.connection = self._get_connection(self.config_version)
    def _get_connection(self, config_version):
        config_content = self.config[config_version]
        return pymysql.connect(host=config_content["host_info"]["host"],
                             port=config_content["host_info"]["port"],
                             user=config_content["user_info"]["username"],
                             password=config_content["user_info"]["password"],
                             db=config_content["db_name"],
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
    def reset_connection(self):
        self.connection.close()
        self.connect()
