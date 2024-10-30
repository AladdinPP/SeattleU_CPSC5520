"""
Author:     Hongru He
Filename:   lab2.py
Date:       10/10/2024
Purpose:    Initialize the Client instances of a group of nodes in the bully
            algorithm and execute the interaction with the running GCD and
            other group members.
Usage:      python3 lab2.py GCDHOST GCDPORT SUID [DOB]
            - GCDHOST: The hostname or IP address of the Group Coordination Daemon (GCD).
            - GCDPORT: The port number of the GCD.
            - SUID:    A Seattle University ID (should be in the range 1000000 to 9999999).
            - DOB:     (Optional) Mother's birthday in the format mm-dd. Defaults to "01-01".

            Example:
            python3 lab2.py localhost 8080 1234567 05-10

            This command will start a node in the bully algorithm, registering it with the GCD,
            and initiating an election if necessary.
Input:      - Command-line arguments:
              1. GCDHOST: Hostname or IP of the GCD.
              2. GCDPORT: Port number of the GCD.
              3. SUID:    SeattleU ID in the range of 1000000 to 9999999.
              4. DOB:     (Optional) Mother's birthday (mm-dd), used to calculate days until
                          the next birthday. Defaults to "01-01" if not provided.
Output:     - The program prints various log messages to the console, including:
              - Server's start information.
              - Election status and related messages (e.g., ELECTION, OK, COORDINATOR).
              - Information about the leader and the current state of the network.
"""

import socket
import threading
import socketserver
import pickle
import time
import random
import sys
from datetime import datetime
import logging

class Lab2TCPServer(socketserver.ThreadingTCPServer):
    """
    This class represents a TCP server that is used to manage communication
    between nodes participating in the Bully algorithm. It inherits from
    `socketserver.ThreadingTCPServer`, allowing each client request to be
    handled in a separate thread. This class encapsulates all functionalities
    related to client management, elections, and leader coordination.

    Attributes:
        gcd_host (str):     The hostname of the Group Coordination Daemon (GCD).
        gcd_port (int):     The port number of the GCD.
        identity (tuple):   A unique identity for the server, consisting of
                            (days_to_next_mother's_birthday, su_id).
        in_election (bool): Flag to determine if the node is currently
                            in an election.
        current_leader (tuple or None): The identity of the current leader node.
        members (dict):     A dictionary of other group members in the form
                            {identity: (host, port)}.
        lock (threading.Lock):  A lock to ensure thread-safe access to
                                shared data structures like `members`.
    """

    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass, gcd_host, gcd_port, identity):
        """
        Initialize the Lab2TCPServer instance, setting up the server and
        initializing its attributes.

        Args:
            server_address (tuple): The address on which the server will bind.
            RequestHandlerClass (class):    The handler class used to manage
                                            incoming requests.
            gcd_host (str):         Hostname of the GCD server.
            gcd_port (int):         Port number of the GCD server.
            identity (tuple):       The unique identity of this node.
        """

        super().__init__(server_address, RequestHandlerClass)
        self.gcd_host = gcd_host
        self.gcd_port = gcd_port
        self.identity = identity  # (days_to_next_mother's_birthday, su_id)
        self.in_election = False
        self.current_leader = None
        self.members = {}  # {identity: (host, port)}
        self.lock = threading.Lock()  # For thread-safe operations

    def connect_to_gcd(self):
        """
        Connect to the GCD server to retrieve the list of group members.
        This method sends a 'BEGIN' message to the GCD to register the
        node and get the updated members list.
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.gcd_host, self.gcd_port))
                logging.info(f"Connecting to GCD at {self.gcd_host}:{self.gcd_port}")
                begin_message = ('BEGIN', (self.identity, self.server_address))
                s.sendall(pickle.dumps(begin_message))
                response = s.recv(4096)
                response_data = pickle.loads(response)
            if isinstance(response_data, dict):
                with self.lock:
                    self.members = response_data
                logging.info(f"Members received from GCD: {self.members}")
            else:
                logging.info(f"Unexpected response from GCD: {response_data}")
        except Exception as e:
            logging.error(f"Failed to connect to GCD: {e}")

    def start_election(self):
        """
        Start an election by sending 'ELECTION' messages to all higher
        identity members. Waits for 'OK' messages and potentially declares
        self as leader if no 'OK's are received.
        """
        with self.lock:
            if self.in_election:
                logging.info("Already in an election.")
                return
            self.in_election = True
            logging.info(f"Starting election for identity {self.identity}")

        self.connect_to_gcd()  # Get the latest members list

        with self.lock:
            # Select members with higher identities
            higher_members = {ident: addr for ident, addr in self.members.items() if ident > self.identity}

        if not higher_members:
            logging.info("No higher members found. Declaring self as leader.")
            self.declare_leader()
        else:
            logging.info(f"Higher members found: {higher_members.keys()}")
            # Send ELECTION message to higher members
            for ident, addr in higher_members.items():
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(5)
                        s.connect(addr)
                        election_message = ('ELECTION', self.identity,
                                            self.members)
                        s.sendall(pickle.dumps(election_message))
                        logging.info(f"Sent ELECTION message to {ident} at {addr}")
                except Exception as e:
                    logging.error(f"Failed to send ELECTION to {ident} at {addr}: {e}")
            # Wait for OK messages
            self.handle_ok()

    def handle_ok(self):
        """
        Wait for an 'OK' message from a higher node or time out to declare
        itself as the leader. If an 'OK' message is received, the node
        waits for a 'COORDINATOR' message from the new leader.
        """
        timeout = 5  # seconds
        self.received_ok_event = threading.Event()

        logging.info("Waiting for OK message or timeout...")

        ok_received = self.received_ok_event.wait(timeout)
        if ok_received:
            logging.info("Received OK message. Waiting for COORDINATOR message.")
            self.wait_for_coordinator()
        else:
            logging.info("Timeout reached. No OK received.")
            self.declare_leader()

    def handle_ok_received(self):
        """
        Set the event flag when an 'OK' message is received, signaling
        that the node should not declare itself the leader.
        """
        logging.info("Received OK message")

        if hasattr(self, 'received_ok_event'):
            self.received_ok_event.set()
            logging.info("OK message received and event set.")
        else:
            logging.info("Received OK message, but no event to set.")

    def handle_election(self, sender_identity, sender_address, sender_members):
        """
        Handle receipt of an 'ELECTION' message. The node responds with
        an 'OK' message to the sender and starts its own election if
        not already in one.

        Args:
            sender_identity (tuple): The identity of the node that sent the election message.
            sender_address (tuple): The address of the sender node.
            sender_members (dict): The members list from the sender node.
        """
        logging.info(f"Received ELECTION message from {sender_identity} at {sender_address}")

        with self.lock:
            # Update members list with sender's members
            self.members.update(sender_members)
            logging.info(f"Updated members list with sender's members: {self.members}")

        # Send back OK message to sender
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(sender_address)
                ok_message = ('OK', None)
                s.sendall(pickle.dumps(ok_message))
                logging.info(f"Sent OK message to {sender_identity} at {sender_address}")
        except Exception as e:
            logging.error(f"Failed to send OK to {sender_identity} at {sender_address}: {e}")

        # If not already in an election, start one
        if not self.in_election:
            logging.info("Not in election. Starting own election.")
            self.start_election()

    def declare_leader(self):
        """
        Declare the current node as the leader and notify all other members
        by sending them a 'COORDINATOR' message.
        """
        with self.lock:
            self.current_leader = self.identity
            self.in_election = False
            logging.info(f"Declaring self ({self.identity}) as leader.")

        # Uncomment the following line to introduce a delay for testing purposes
        # time.sleep(10)  # Introduce delay before sending COORDINATOR messages

        # Send COORDINATOR message to all members except self
        for ident, addr in self.members.items():
            if ident == self.identity:
                continue
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect(addr)
                    coordinator_message = ('COORDINATOR', self.identity)
                    s.sendall(pickle.dumps(coordinator_message))
                    logging.info(f"Sent COORDINATOR message to {ident} at {addr}")
            except Exception as e:
                logging.error(f"Failed to send COORDINATOR to {ident} at {addr}: {e}")
                continue

    def handle_leader(self, new_leader_identity):
        """
        Handle receipt of a 'COORDINATOR' message, indicating the new leader.

        Args:
            new_leader_identity (tuple): The identity of the new leader.
        """
        logging.info(f"Received COORDINATOR message from {new_leader_identity}")

        with self.lock:
            self.current_leader = new_leader_identity
            self.in_election = False
            logging.info(f"Updated leader to {new_leader_identity}")

        if hasattr(self, 'received_coord_event'):
            self.received_coord_event.set()
            logging.info("COORDINATOR message received and event set.")

    def wait_for_coordinator(self):
        """
        Wait for a 'COORDINATOR' message from the new leader or time out to
        start a new election.
        """
        timeout = 15  # seconds
        self.received_coord_event = threading.Event()

        logging.info("Waiting for COORDINATOR message or timeout...")

        coord_received = self.received_coord_event.wait(timeout)
        if coord_received:
            logging.info("Received COORDINATOR message.")
            # Do nothing, leader has been updated in handle_leader()
        else:
            logging.info("Timeout reached. No COORDINATOR message received.")
            # Reset in_election flag to allow starting a new election
            with self.lock:
                self.in_election = False
            # Start a new election
            self.start_election()

    def handle_request(self, message):
        """
        Handle incoming messages by routing them to the appropriate handler
        based on the message type.

        Args:
            message (tuple): The incoming message, which consists of
                             (message_name, message_data, members_data).
        """
        message_name = message[0]
        message_data = message[1] if len(message) > 1 else None
        members_data = message[2] if len(message) > 2 else None

        if message_name == 'ELECTION':
            sender_identity = message_data
            sender_address = members_data.get(sender_identity)
            self.handle_election(sender_identity, sender_address, members_data)
        elif message_name == 'COORDINATOR':
            new_leader_identity = message_data
            self.handle_leader(new_leader_identity)
        elif message_name == 'OK':
            self.handle_ok_received()
        else:
            logging.info(f"Unknown message type: {message_name}")

class Lab2TCPRequestHandler(socketserver.BaseRequestHandler):
    """
    The request handler for the TCP server. This class handles the actual
    processing of incoming messages for each client in a separate thread.

    Methods:
        handle(): Receives and processes the incoming request.
    """

    def handle(self):
        peer_address = self.client_address
        try:
            data = self.request.recv(4096)
            message = pickle.loads(data)
            logging.info(f"Received {message[0]} message from {peer_address}")
            self.server.handle_request(message)
        except Exception as e:
            logging.error(
                f"Exception while handling message from {peer_address}: {e}")

if __name__ == "__main__":
    # Set up logging with timestamps
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(threadName)s] %(message)s'
    )

    # Parse command line arguments
    if len(sys.argv) < 4:
        print("Usage: python3 lab2.py GCDHOST GCDPORT SUID [DOB]")
        sys.exit(1)

    gcd_host = sys.argv[1]
    gcd_port = int(sys.argv[2])
    su_id = int(sys.argv[3])
    if su_id < 1000000 or su_id > 9999999:
        print("SU_ID must be in the range of 1000000 to 9999999.")
        sys.exit(1)

    # Determine mother's birthday
    if len(sys.argv) == 5:
        mom_birthday_str = sys.argv[4]
    else:
        mom_birthday_str = "01-01"  # Default to the closest next 01-01

    try:
        mom_birthday = datetime.strptime(mom_birthday_str, "%m-%d").replace(
            year=datetime.now().year)
        if mom_birthday < datetime.now():
            mom_birthday = mom_birthday.replace(year=datetime.now().year + 1)
        days_to_mom_birthday = (mom_birthday - datetime.now()).days
    except ValueError:
        print("Invalid date format. Use mm-dd.")
        sys.exit(1)

    # Set identity based on (days_to_mom_birthday, SU_ID)
    identity = (days_to_mom_birthday, su_id)

    HOST, PORT = 'localhost', 0  # Bind to localhost and let the OS choose an available port

    server = Lab2TCPServer((HOST, PORT), Lab2TCPRequestHandler, gcd_host, gcd_port, identity)
    HOST, PORT = server.server_address  # Get the dynamically assigned port

    # Start the server in a separate thread
    try:
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        logging.info(f"Server running at {HOST}:{PORT} with identity {identity}")
    except Exception as e:
        logging.error(f"Failed to start server thread: {e}")
        sys.exit(1)

    logging.info(f"Next Birthday: {mom_birthday.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"SeattleU ID: {su_id}")
    logging.info(f"Server loop running in thread: {server_thread.name}")
    logging.info(f"Using GCD at {gcd_host}:{gcd_port}")

    # Start an election after a short delay
    time.sleep(1)
    server.start_election()

    try:
        while True:
            time.sleep(random.uniform(1, 5))
    except KeyboardInterrupt:
        logging.info("Shutting down server...")
        server.shutdown()
        server.server_close()