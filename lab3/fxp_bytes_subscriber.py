"""
Author:    Hongru He
Date:      10/25/2024
Filename:  fxp_bytes_subscriber.py
Purpose:   Subscriber-side utility functions for handling Forex Provider messages.
           This module provides functions to serialize and deserialize data exchanged
           with the Forex Provider, including addresses and messages containing exchange rate quotes.
Input:     Byte streams received from the Forex Provider containing exchange rate quotes.
Output:    Parsed data structures representing the exchange rate quotes.
"""

import socket
import struct
from datetime import datetime, timedelta

MICROS_PER_SECOND = 1_000_000

def serialize_address(address):
    """
    Serialize the subscriber's address (IP and port) into the format required by the Forex Provider.

    Format:
    - 4-byte IPv4 address in big-endian (network format)
    - 2-byte port number in big-endian

    :param address: (ip_address, port)
    :return: bytes
    """
    ip_str, port = address
    ip_bytes = socket.inet_aton(ip_str)  # Convert IP to 4-byte binary
    port_bytes = port.to_bytes(2, byteorder='big')  # Port number in big-endian
    return ip_bytes + port_bytes

def deserialize_price(price_bytes):
    """
    Deserialize the price from the 4-byte little-endian IEEE 754 binary32 format.

    :param price_bytes: 4-byte little-endian float
    :return: float price
    """
    # '<f' format specifier for little-endian float
    price = struct.unpack('<f', price_bytes)[0]
    return price

def unmarshal_message(message):
    """
    Parse the byte stream received in a Forex Provider message into a list of quote dictionaries.

    Each 32-byte record format:
    - Bytes 0-2: Currency 1 (3 bytes ASCII)
    - Bytes 3-5: Currency 2 (3 bytes ASCII)
    - Bytes 6-9: Exchange rate (4-byte little-endian float)
    - Bytes 10-17: Timestamp (8-byte big-endian unsigned long long)
    - Bytes 18-31: Reserved (14 bytes, ignored)

    :param message: byte stream received from the Forex Provider
    :return: list of quote dictionaries with 'cross', 'price', 'time'
    """
    quotes = []
    record_size = 32
    num_quotes = len(message) // record_size
    for i in range(num_quotes):
        offset = i * record_size
        record = message[offset:offset + record_size]

        # Currencies
        curr_a = record[0:3].decode('ascii')
        curr_b = record[3:6].decode('ascii')
        cross = f"{curr_a}/{curr_b}"

        # Exchange rate
        price_bytes = record[6:10]
        price = deserialize_price(price_bytes)

        # Timestamp
        timestamp_bytes = record[10:18]
        micros_since_epoch = int.from_bytes(timestamp_bytes, byteorder='big')
        timestamp = datetime(1970, 1, 1) + timedelta(microseconds=micros_since_epoch)

        quote = {'cross': cross, 'price': price, 'time': timestamp}
        quotes.append(quote)
    return quotes