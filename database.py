# database.py master

import pymysql
import logging
from datetime import datetime

class DatabaseConnection:
    """Handles database connections."""
    def __init__(self, config):
        self.config = config
        self.connection = None

    def __enter__(self):
        try:
            self.connection = pymysql.connect(**self.config)
            return self.connection
        except pymysql.MySQLError as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

class DatabaseOperations:
    """Performs database operations."""
    
    @staticmethod
    def insert_data(data, table_name, connection):
        if not data:
            logging.warning("No data provided for insertion.")
            return False

        try:
            with connection.cursor() as cursor:
                for record in data:
                    if table_name == "slaves_modem":
                        record.pop('created_date', None)
                        record.pop('updated_at', None)
                        record.pop('modem_id', None)
                    elif table_name == "slaves_equipment":
                        record.pop('created_at', None)
                        record.pop('updated_at', None)
                        record.pop('equipment_id', None)
                    elif table_name == "live_data":
                        record.pop('timestamp', None)

                    keys = ', '.join(f"{key}" for key in record.keys())
                    placeholders = ', '.join(['%s'] * len(record))
                    updates = ', '.join([f"{key}=VALUES({key})" for key in record.keys()])
                    sql = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
                    cursor.execute(sql, tuple(record.values()))
            connection.commit()
            logging.info(f"Data successfully inserted into '{table_name}'.")
            return True
        except pymysql.MySQLError as e:
            logging.error(f"Error inserting data into '{table_name}': {e}")
            connection.rollback()
            return False

    @staticmethod
    
    def fetch_data(table_name, connection, slave_mac=None):
        """Fetch data from the specified table with an optional slave_mac filter."""
        try:
            with connection.cursor() as cursor:
                # query = '''SELECT * FROM slaves_modem  WHERE modem_serial_no = "02:81:08:b1:66:b6"  '''
                # cursor.execute(query)
                # print(f"a {cursor.fetchall()}")
                
                query = f"SELECT * FROM {table_name}"
                params = []
                if slave_mac:
                    mac_columns = {
                        'slaves_modem': 'modem_serial_no',  
                        'slaves_equipment': 'modem_serial_no',  
                    }
                    mac_column = mac_columns.get(table_name)

                   # res modem_serial_no
                    
                    if mac_column:
                        query += f" WHERE {mac_column} = %s"
                        params.append(slave_mac)
                    else:
                        logging.warning(f"No MAC address column defined for table {table_name}")
                        return []
                cursor.execute(query, params)
                print(f"query {query}") #*************************************
                print(f"params {params}")
                
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Error fetching data from {table_name}: {e}")
            return []
            

    @staticmethod
    def get_master_token(connection):
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT token FROM table_version WHERE table_name = %s", ('modem_slaves'))
                result = cursor.fetchone()
                if result and 'token' in result:
                    logging.info(f"Master token retrieved: {result['token']}")
                    return result['token']
                else:
                    logging.error("Master token not found in the database.")
                    return None
        except pymysql.MySQLError as e:
            logging.error(f"Error retrieving master token: {e}")
            return None
##