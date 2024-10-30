"""Example threaded server based on example at end of python.org's
socketserver documentation page.
see: https://docs.python.org/3/library/socketserver.html

Changes to their example:
o Use the predefined socketserver.ThreadingTCPServer instead of mixing our own
o Use the shared server object to demo IPC with shared memory (server.notes)

Kevin Lundeen for SeattleU/CPSC5520 f23
"""
import socket
import threading
import socketserver


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        data = str(self.request.recv(1024), 'ascii')
        cur_thread = threading.current_thread()
        response = "{}: {}".format(cur_thread.name, data)
        self.request.sendall(bytes(response, 'ascii'))

        # self is a different object for each thread, but it has references
        # to the server's object, too, so we can share that
        self.server.notes.append(response)  # append to a list is thread-safe


def client(ip, port, message):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((ip, port))
        sock.sendall(bytes(message, 'ascii'))
        response = str(sock.recv(1024), 'ascii')
        print("Received: {}".format(response))


if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "localhost", 0

    server = socketserver.ThreadingTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    server.notes = []  # set up a place for all the threads to add notes
    with server:
        ip, port = server.server_address

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        print("Server loop running in thread:", server_thread.name)

        client(ip, port, "Hello World 1")
        client(ip, port, "Hello World 2")
        client(ip, port, "Hello World 3")

        print('\nHere are the notes appended by each thread:')
        for note in server.notes:
            print(note)  # print out the notes appended by each thread
        server.shutdown()