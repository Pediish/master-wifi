# config.py master

import os
import pymysql
import logging
from getmac import get_mac_address
import socket
import subprocess

def mac_to_ip(mac_address):
    """Convert a MAC address to an IP address."""
    try:
        # Use the arp command to find the IP address associated with the MAC address
        result = subprocess.run(['arp', '-n'], capture_output=True, text=True)
        for line in result.stdout.splitlines():
            if mac_address.lower() in line.lower():
                return line.split()[0]  # Extract the IP address
    except Exception as e:
        logging.error(f"Error retrieving IP for MAC {mac_address}: {e}")
    return "0.0.0.0"  # Default IP if not found

class Config:
    """Configuration settings for the application."""
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', '123'),
        'db': os.getenv('DB_NAME', 'IoTLocal'),
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    MAC_ADDRESSES = [
        os.getenv('SLAVE_MAC_1', '12:81:08:b1:66:b6'),
        os.getenv('SLAVE_MAC_2', '12:81:f8:16:b9:f9'),
        os.getenv('SLAVE_MAC_3', '12:81:d7:01:cb:11')
    ]
    @classmethod
    def get_slave_urls(cls):
        """Generate slave URLs based on MAC addresses."""
        return [f'http://{mac_to_ip(mac)}:5100' for mac in cls.MAC_ADDRESSES]

    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', '123'),
        'db': os.getenv('DB_NAME', 'IoTLocal'),
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '10'))  # in seconds
    COMPARE_INTERVAL = int(os.getenv('COMPARE_INTERVAL', '10'))  # in seconds

class Logger:
    """Logger configuration."""
    @staticmethod
    def configure():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve slave URLs after class definition
Config.SLAVE_URLS = Config.get_slave_urls()