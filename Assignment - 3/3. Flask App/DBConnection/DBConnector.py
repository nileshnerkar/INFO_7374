import psycopg2
import json
class DBConnector():
    def __init__(self):
        with open('DBConnection\dbConfig.json', 'rb') as config_file:
            config = json.load(config_file)
        
        try:
            self.__connection = psycopg2.connect(
                user=config['username'],
                password = config['password'],
                host=config['host'],
                port=config['port'],
                database=config['database']
            )
        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)

    @property
    def connection(self):
        return self.__connection
    
    def execute(self, sql):
        try:
            curr = self.__connection.cursor()
            curr.execute(sql)
            rec = curr.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            return
        return rec