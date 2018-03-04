#!/usr/bin/env python
#tpft - tiny proto file transfer
from tinyproto import TinyProtoServer, TinyProtoClient, TinyProtoConnection
import argparse


#### Server classes ####
class TPFTServerConnection(TinyProtoConnection):
    def transmission_received(self, msg):
        print('Received transmission: {0}'.format(msg))
        self.shutdown = True


class TPFTServer(TinyProtoServer):
    __slots__ = TinyProtoServer.__slots__ + ('args',)

    def set_args(self, a):
        self.args = a

    def conn_init(self, conn_o):
        if self.args.verbose:
            remote_details = conn_o.socket_o.getpeername()
            print('Connection opened from: {0}'.format(remote_details))
    def conn_shutdown(self, conn_h):
        if self.args.verbose:
            print('Connection closed from: {0}'.format((conn_h.host, conn_h.port)))

    def start_listening(self):
        self.add_addr(self.args.host, self.args.port)
        self.set_conn_handler(TPFTServerConnection)
        if self.args.verbose:
            print('Starting listening service and awaiting connections.')
        self.start()

#### Client ####
class TPFTClientConnection(TinyProtoConnection):
    def loop_pass(self):
        self.transmit('Got connection!!')
        self.shutdown = True

class TPFTClient(TinyProtoClient):
    __slots__ = TinyProtoServer.__slots__ + ('args',)

    def set_args(self, a):
        self.args = a

    def pre_loop(self):
        self.set_conn_handler(TPFTClientConnection)
        self.connect_to(self.args.host, self.args.port)
        if self.args.verbose:
            print('Starting connection ... ')

    def loop_pass(self):
        #if len(self.active_connections) == 0:
        if len([True for x in self.active_connections if x.is_alive()]) == 0:
            self.shutdown = True

    def post_loop(self):
        if self.args.verbose:
            print('Connection finished, shutting down client.')


arg_parser = argparse.ArgumentParser('Client/Server file transfer tool.')
main_group = arg_parser.add_mutually_exclusive_group(required=True)
main_group.add_argument('-l', '--listen', help='Start a server and listen for connections on specified host:port', action='store_true')
main_group.add_argument('-d', '--download', help='Download a file', action='store_true')
main_group.add_argument('-u', '--upload', help='Upload a file', action='store_true')
arg_parser.add_argument('--host', help='Host for connection', required=True)
arg_parser.add_argument('--port', help='Port for connection. Default: 9111', default=9111, type=int)
arg_parser.add_argument('--source', help='Source of {download|upload} operation')
arg_parser.add_argument('--destination', help='Destination of {download|upload} operation')
arg_parser.add_argument('--verbose', action='store_true', help='Display additional messages')
arg_parser.add_argument('--progress', action='store_true', help='Display progress of transfer')



if __name__ == '__main__':
    args = arg_parser.parse_args()

    if args.listen:
        runner = TPFTServer()
        runner.set_args(args)
        runner.start_listening()
    if args.download or args.upload:
        connector = TPFTClient()
        connector.set_args(args)
        connector.start()
