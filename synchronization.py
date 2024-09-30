# synchronization.py master
import requests
import logging
import time
from datetime import datetime
from threading import Thread
from database import DatabaseConnection, DatabaseOperations
from config import Config

class DataSerializer:

    @staticmethod
    def serialize(data):
        serialized = []
        for record in data:
            serialized_record = {}
            for key, value in record.items():
                if isinstance(value, datetime):
                    serialized_record[key] = value.isoformat()
                else:
                    serialized_record[key] = value
            serialized.append(serialized_record)
        return serialized

class SlaveCommunicator:

    @staticmethod
    def get_slave_token(slave_url):
        try:
            response = requests.get(f"{slave_url}/get_token", timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            slave_token = data.get('token')
            if slave_token is not None:
                logging.info(f"Slave token retrieved from {slave_url}: {slave_token}")
                return slave_token
            else:
                logging.error(f"Slave token not found in the response from {slave_url}.")
                return None
        except requests.Timeout:
            logging.error(f"Request to slave {slave_url} timed out.")
            return None
        except requests.RequestException as e:
            logging.error(f"HTTP request to slave {slave_url} failed: {e}")
            return None

    @staticmethod

    def send_data(slave_url, table_name, data):
        try:
            serialized_data = DataSerializer.serialize(data)
            url = f"{slave_url}/receive_data/{table_name}"
            payload = {'data': serialized_data}
            logging.debug(f"Sending data to {url}: {payload}")
            response = requests.post(url, json=payload, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            logging.info(f"Data sent to slave {slave_url} for table '{table_name}'. Response: {response.status_code} - {response.text}")
            return True
        except requests.RequestException as e:
            logging.exception(f"Failed to send data to slave {slave_url} for table '{table_name}': {e}")
            return False

    @staticmethod
    def retrieve_data(slave_url, table_name):
        try:
            url = f"{slave_url}/send_data/{table_name}"
            response = requests.get(url, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json().get('data', [])
            if not data:
                logging.warning(f"No data received from slave {slave_url} for table '{table_name}'.")
                return []
            return data
        except requests.Timeout:
            logging.error(f"Request to retrieve data from slave {slave_url} timed out.")
            return []
        except requests.RequestException as e:
            logging.error(f"Failed to retrieve data from slave {slave_url} for table '{table_name}': {e}")
            return []
        except ValueError:
            logging.error(f"Invalid JSON response from slave {slave_url}.")
            return []

class Synchronizer:

    def __init__(self):
        self.table_mappings = {
            'modem': 'slaves_modem',
            'equipment': 'slaves_equipment' 
        }
        self.stop_flag = False

    def compare_and_sync(self):
        while not self.stop_flag:
            logging.info("Starting token comparison cycle.")
            try:
                with DatabaseConnection(Config.DB_CONFIG) as connection:
                    master_token = DatabaseOperations.get_master_token(connection)
        
                    slave_url_map = dict(zip(Config.MAC_ADDRESSES, Config.get_slave_urls()))
                                        
                    for slave_mac, slave_url in slave_url_map.items():
                        slave_token = SlaveCommunicator.get_slave_token(slave_url)
                        
                        
                        if master_token is None or slave_token is None:
                            logging.warning(f"Missing tokens for slave {slave_url}. Skipping synchronization.")
                            continue

                        
                        if int(master_token) > int(slave_token):
                            logging.info(f"Sending data to slave {slave_url}.")
                            self.sync_data(slave_url, connection, slave_mac, action='send')
                        elif master_token < slave_token:
                            logging.info(f"Retrieving data from slave {slave_url}.")
                            self.sync_data(slave_url, connection, slave_mac, action='retrieve')
                        else:
                            logging.info(f"No synchronization needed for slave {slave_url}.")
            except Exception:
                logging.exception("An unexpected error occurred during synchronization.")

            logging.info(f"Waiting for {Config.COMPARE_INTERVAL} seconds before next comparison cycle.")
            time.sleep(Config.COMPARE_INTERVAL)    
 
 
    def sync_data(self, slave_url, connection, slave_mac, action):
        for endpoint_table, master_table in self.table_mappings.items():
            
            # print(endpoint_table)
            # print(master_table)            
            # print(action)
            # print(slave_mac) #************************************
             

            if action == 'send':
                data = DatabaseOperations.fetch_data(master_table, connection, slave_mac)
                #print(data)
                if data:
                    SlaveCommunicator.send_data(slave_url, endpoint_table, data)
                    logging.info(f"Data to be sent to {slave_url} for table {endpoint_table}: data")

            
            elif action == 'retrieve':
                data = SlaveCommunicator.retrieve_data(slave_url, endpoint_table)
                if data:
                    DatabaseOperations.insert_data(data, master_table, connection)
                    logging.info(f"Data retrieved from {slave_url} for table {endpoint_table}: {data}")

            elif action == 'update':
                # Fetch specific data that needs to be sent to the slave
                master_data = DatabaseOperations.fetch_data(master_table, connection)  # Fetch all data for the table
                if master_data:
                    SlaveCommunicator.send_data(slave_url, endpoint_table, master_data)  # Send the fetched data


    def start(self):
        self.stop_flag = False
        sync_thread = Thread(target=self.compare_and_sync, daemon=True)
        sync_thread.start()
        logging.info("Synchronization started.")

    def stop(self):
        self.stop_flag = True
        logging.info("Synchronization stopped.")