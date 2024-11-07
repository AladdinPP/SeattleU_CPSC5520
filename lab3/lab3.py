"""
Author:    Hongru He
Date:      10/25/2024
Filename:  lab3.py
Purpose:   Detect arbitrage opportunities in the forex market by subscribing to a forex price feed,
           building a currency graph, and identifying negative cycles using the Bellman-Ford algorithm.
Input:     Command-line argument specifying the publisher's port number.
Output:    Printed exchange rates received from the publisher, arbitrage opportunities when detected,
           and the profit from executing the arbitrage sequence.
"""

import socket
import fxp_bytes_subscriber
from datetime import datetime, timedelta
import math
import time
from bellman_ford import BellmanFord
import sys

QUOTE_EXPIRY = timedelta(seconds=1.5)  # Define the quote expiry time

class ForexSubscriber:
    """
    ForexSubscriber class encapsulates the functionality of subscribing to a forex price feed,
    processing received exchange rate quotes, building a currency graph, and detecting arbitrage
    opportunities using the Bellman-Ford algorithm.
    """

    def __init__(self, publisher_port):
        """
        Initialize the ForexSubscriber.

        Parameters:
            publisher_port (int): The port number of the forex price feed
            publisher.

        Initializes:
            self.publisher_port: Port number of the publisher.
            self.REQUEST_ADDRESS: Address tuple ('localhost', publisher_port).
            self.sock: UDP socket for receiving messages.
            self.local_address: Local address and port of the subscriber.
            self.quotes_dict: Dictionary to store the latest exchange rate
            quotes.
            self.latest_timestamps: Dictionary to track the latest timestamp
            for each currency pair.
            self.last_message_time: Timestamp of the last received message.
        """

        self.publisher_port = publisher_port
        self.REQUEST_ADDRESS = ('localhost', self.publisher_port)

        # Create a UDP socket to receive messages
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.sock.settimeout(10)  # Set a timeout for receiving messages
        self.local_address = self.sock.getsockname()

        self.quotes_dict = {}        # To store the latest quotes
        self.latest_timestamps = {}  # To track the latest timestamp for each market

        # Initialize other necessary variables
        self.last_message_time = datetime.utcnow()

    def send_subscribe(self):
        """
        Send a subscription request to the forex price feed publisher.

        The subscription request includes the subscriber's local address and port.
        """

        data = fxp_bytes_subscriber.serialize_address(self.local_address)
        request_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        request_socket.sendto(data, self.REQUEST_ADDRESS)
        request_socket.close()
        print(f"Subscribed to Forex Publisher with local address {self.local_address}")

    def receive_messages(self):
        """
        Receive messages (exchange rate quotes) from the publisher.

        Returns:
            quotes (list): A list of quote dictionaries received from the publisher.
                           Each quote contains 'cross', 'price', and 'time'.

        If no message is received before the timeout, an empty list is returned.
        """

        try:
            message, addr = self.sock.recvfrom(4096)
            quotes = fxp_bytes_subscriber.unmarshal_message(message)
            return quotes
        except socket.timeout:
            return []

    def process_quotes(self, quotes):
        """
        Process received exchange rate quotes, updating the quotes dictionary and timestamps.

        Parameters:
            quotes (list): A list of quote dictionaries to process.

        The method:
            - Checks for out-of-sequence messages and ignores them.
            - Updates the latest timestamp for each currency pair.
            - Prints the received quote.
            - Updates the quotes dictionary with the new quote.
        """

        for quote in quotes:
            cross = quote['cross']
            price = quote['price']
            timestamp = quote['time']

            # Check for out-of-sequence messages
            if cross in self.latest_timestamps and timestamp < self.latest_timestamps[cross]:
                print(f"{timestamp} {cross.replace('/', ' ')} {price}")
                print("ignoring out-of-sequence message")
                continue

            # Update the latest timestamp for this market
            self.latest_timestamps[cross] = timestamp

            # Print the received quote
            print(f"{timestamp} {cross.replace('/', ' ')} {price}")

            # Update the quotes dictionary
            self.quotes_dict[cross] = {'price': price, 'time': timestamp}

    def remove_stale_quotes(self):
        """
        Remove stale quotes from the quotes dictionary.

        A quote is considered stale if it is older than the defined QUOTE_EXPIRY time.
        """

        current_time = datetime.utcnow()
        for cross in list(self.quotes_dict.keys()):
            quote_time = self.quotes_dict[cross]['time']
            if current_time - quote_time > QUOTE_EXPIRY:
                print(f"removing stale quote for {cross}")
                del self.quotes_dict[cross]

    def build_graph(self):
        """
        Build the currency graph using the current exchange rate quotes.

        Returns:
            bf (BellmanFord): An instance of the BellmanFord class representing the currency graph.

        The graph's edges are weighted by the negative logarithm of the exchange rates.
        """

        bf = BellmanFord()
        for cross in self.quotes_dict:
            rate = self.quotes_dict[cross]['price']
            curr_a, curr_b = cross.split('/')
            # Use the negative logarithm of the exchange rate as the edge weight
            weight = -math.log(rate)

            # Add the forward edge
            bf.add_edge(curr_a, curr_b, weight)

            # Add the reverse edge
            reverse_weight = -math.log(1 / rate)
            bf.add_edge(curr_b, curr_a, reverse_weight)
        return bf

    def find_arbitrage(self, bf):
        """
        Find arbitrage opportunities using the Bellman-Ford algorithm starting
        from USD.

        Parameters:
            bf (BellmanFord): The currency graph.

        Returns:
            negative_cycle_edge (tuple): A tuple (u, v) representing an edge in
                                         a detected negative cycle.
            predecessor (dict): A dictionary mapping each vertex to its
                                predecessor in the shortest path.

        If no negative cycle is detected, (None, None) is returned.
        """

        start_currency = 'USD'
        if start_currency not in bf.vertices:
            return None, None

        distance, predecessor, negative_cycle_edge = bf.shortest_paths(
            start_currency)
        if negative_cycle_edge:
            cycle = self.reconstruct_negative_cycle(negative_cycle_edge,
                                                    predecessor)
            if cycle is None:
                return None, None

            total_weight = 0
            for i in range(len(cycle) - 1):
                u = cycle[i]
                v = cycle[i + 1]
                weight = bf.edges[u][v]
                total_weight += weight

            # Define a small negative threshold (epsilon)
            EPSILON = -1e-8
            if total_weight >= EPSILON:
                return None, None

            return negative_cycle_edge, predecessor
        return None, None

    def reconstruct_negative_cycle(self, negative_cycle_edge, predecessor):
        """
        Reconstruct the negative cycle detected by the Bellman-Ford algorithm.

        Parameters:
            negative_cycle_edge (tuple): The edge (u, v) where a negative cycle
                                         was detected.
            predecessor (dict): The predecessor dictionary from the Bellman-Ford
                                algorithm.

        Returns:
            cycle (list): A list of vertices representing the negative cycle.

        The cycle is constructed by backtracking through the predecessors
        starting from the edge.
        """

        u, v = negative_cycle_edge
        cycle = [v]
        current = u
        while current not in cycle:
            cycle.append(current)
            current = predecessor.get(current)
            if current is None:
                # Cannot trace back further
                return None
        cycle.append(current)
        cycle = cycle[cycle.index(current):]  # Get the cycle part
        cycle.reverse()
        return cycle

    def display_arbitrage(self, cycle):
        """
        Display the arbitrage opportunity with detailed exchange steps and
        profit.

        Parameters:
            cycle (list): A list of currencies forming the arbitrage cycle.

        The method:
            - Attempts both the original and reversed directions of the cycle.
            - Calculates the profit for each direction.
            - Displays the arbitrage opportunity if a positive profit is found.
            - If no profitable direction is found, reports that no profitable
              arbitrage opportunity exists.
        """

        # Ensure the cycle includes USD
        if 'USD' not in cycle:
            print("Negative cycle does not include USD. Skipping.")
            return

        # Try both directions
        for direction in ['original', 'reversed']:
            if direction == 'original':
                test_cycle = cycle[:]
            else:
                test_cycle = cycle[::-1]

            # Rotate the cycle to start with USD
            while test_cycle[0] != 'USD':
                test_cycle.append(test_cycle.pop(0))

            # Calculate profit
            profit, amount_log = self.calculate_profit(test_cycle)
            if profit is not None and profit >= 0.01:
                self.print_arbitrage(test_cycle, amount_log, profit)
                return  # Exit after finding a profitable arbitrage

        print("No profitable arbitrage opportunity found.")

    def calculate_profit(self, cycle):
        """
        Calculate the profit for the given cycle.

        Parameters:
            cycle (list): A list of currencies forming the arbitrage cycle.

        Returns:
            profit (float): The profit obtained from executing the arbitrage
                            sequence.
            amount_log (list): A list of strings detailing each exchange step.

        The method calculates the amount obtained at each exchange step,
        starting with a fixed amount of 100 units of the starting currency.
        """

        starting_amount = 100.0
        amount = starting_amount
        amount_log = [f"\tstart with {cycle[0]} {amount}"]
        for i in range(len(cycle) - 1):
            curr_from = cycle[i]
            curr_to = cycle[i + 1]
            cross = f"{curr_from}/{curr_to}"
            reverse_cross = f"{curr_to}/{curr_from}"

            if cross in self.quotes_dict:
                rate = self.quotes_dict[cross]['price']
                rate_direction = 'direct'
            elif reverse_cross in self.quotes_dict:
                rate = 1 / self.quotes_dict[reverse_cross]['price']
                rate_direction = 'reverse'
            else:
                # Missing rate; cannot complete the cycle
                return None, None

            amount *= rate
            amount_log.append(f"\texchange {curr_from} for {curr_to} at {rate} ({rate_direction}) --> {curr_to} {amount}")

        profit = amount - starting_amount
        return profit, amount_log

    def print_arbitrage(self, cycle, amount_log, profit):
        """
        Print the arbitrage opportunity.

        Parameters:
            cycle (list): The arbitrage cycle.
            amount_log (list): The exchange steps and amounts.
            profit (float): The profit obtained from the arbitrage sequence.

        The method outputs the exchange steps and the total profit in a
        formatted manner.
        """
        
        print("ARBITRAGE:")
        for line in amount_log:
            print(line)
        print(f"\n\tTotal profit: {profit:.2f} {cycle[0]}\n")

    def run(self):
        """
        Main loop to run the subscriber.

        The method:
            - Sends a subscription request to the publisher.
            - Continuously receives and processes exchange rate quotes.
            - Builds the currency graph and looks for arbitrage opportunities.
            - Displays any detected arbitrage opportunities.
            - Terminates after the subscription expires or on keyboard interrupt.
        """

        self.send_subscribe()

        try:
            while True:
                quotes = self.receive_messages()
                if not quotes:
                    # No message received, check if subscription expired
                    if datetime.utcnow() - self.last_message_time > timedelta(minutes=10):
                        print("Subscription expired. Exiting.")
                        break
                    continue

                self.last_message_time = datetime.utcnow()
                self.process_quotes(quotes)
                self.remove_stale_quotes()
                bf = self.build_graph()
                negative_cycle_edge, predecessor = self.find_arbitrage(bf)

                if negative_cycle_edge:
                    cycle = self.reconstruct_negative_cycle(negative_cycle_edge, predecessor)
                    if 'USD' in cycle:
                        self.display_arbitrage(cycle)
                    else:
                        print("Negative cycle does not include USD. Skipping.")
                else:
                    print("No arbitrage opportunity detected at this time.")

                # Sleep briefly to avoid tight loop (optional)
                time.sleep(1)

        except KeyboardInterrupt:
            print("Subscriber stopped.")
        finally:
            self.sock.close()

def main():
    """
    Entry point of the script.

    Parses command-line arguments to obtain the publisher's port number and starts the ForexSubscriber.
    """

    # Parse command-line arguments
    if len(sys.argv) >= 2:
        try:
            publisher_port = int(sys.argv[1])
        except ValueError:
            print("Usage: python3 lab3.py [publisher_port]")
            sys.exit(1)
    else:
        publisher_port = 50403

    subscriber = ForexSubscriber(publisher_port)
    subscriber.run()

if __name__ == '__main__':
    main()