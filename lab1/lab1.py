"""
Author:     Hongru He
Filename:   lab1.py
Date:       09/29/2024
Purpose:    Initialize the GCDClient instance and execute the interaction
            with the possible running GCD.
Usage:      The script should be run from the command line and requires two
            arguments:
            1. GCD host: The hostname of the GCD
            2. GCD port: The port number on which the GCD is listening
Input:      - GCD_HOST: The hostname or IP address of the GCD server.
            - GCD_PORT: The port number on which the GCD is running.
Output:     - Prints a list of group members received from the GCD.
            - For each group member, attempts to connect and sends a 'HELLO'
            message.
            - Prints the response from each group member or handles any
            connection issues.
            - In case of connection failure to any group member, an appropriate
            error message is printed.
"""

import socket
import pickle
import sys

# Define timeout
TIMEOUT = 1.5

class GCDClient:
    def __init__(self, gcd_host, gcd_port):
        """Initialize the client with the GCD's host and port."""
        self.gcd_host = gcd_host
        self.gcd_port = gcd_port
        self.group_members = []

    def connect_to_gcd(self):
        """
        Connect to the GCD and send the BEGIN message to get group members.
        """
        try:
            # Create a TCP socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.gcd_host, self.gcd_port))

                # Send the 'BEGIN' message
                begin_message = pickle.dumps('BEGIN')
                sock.sendall(begin_message)

                # Receive the response
                data = sock.recv(1024)
                self.group_members = pickle.loads(data)
                print(f"Received group members: {self.group_members}")

        except Exception as e:
            print(f"Failed to connect to GCD: {e}")
            return None

    def send_hello_to_member(self, member):
        """Connect to a group member and send 'HELLO' message."""
        host, port = member['host'], member['port']

        try:
            # Create a TCP socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # Set timeout
                sock.settimeout(TIMEOUT)

                # Connect to the group member
                sock.connect((host, port))

                # Send the 'HELLO' message
                hello_message = pickle.dumps('HELLO')
                sock.sendall(hello_message)

                # Receive the response
                response = sock.recv(1024)
                unpickled_response = pickle.loads(response)

                # Print the response
                print(f"HELLO to {member} => {unpickled_response}")

        # Handle the timeout exception
        except socket.timeout:
            print(f"Connection to {member} timed out.")

        # Handle the connection exception
        except Exception as e:
            print(f"Failed to connect to {member}: {e}")

    def greet_group_members(self):
        """Iterate over the list of group members and send 'HELLO' message."""
        if not self.group_members:
            print("No group members found.")
            return

        for member in self.group_members:
            self.send_hello_to_member(member)

def main():
    # Ensure proper usage
    if len(sys.argv) != 3:
        print("Usage: python client.py $GCD_HOST $GCD_PORT")
        sys.exit(1)

    # Parse the command line
    gcd_host = sys.argv[1]
    gcd_port = int(sys.argv[2])

    # Create an instance of GCDClient and run the interaction
    client = GCDClient(gcd_host, gcd_port)
    client.connect_to_gcd()
    client.greet_group_members()

if __name__ == "__main__":
    main()