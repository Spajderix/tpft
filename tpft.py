#!/usr/bin/env python
#tpft - tiny proto file transfer
import os
from tinyproto import TinyProtoServer, TinyProtoClient, TinyProtoConnection
import argparse
import json

CHUNK_SIZE=4*1024*1024

class ProtoPayload(object):
    __slots__ = ('_proto_status', '_proto_op', '_proto_msg')

    def __init__(self):
        self._proto_status = None
        self._proto_op = None
        self._proto_msg = None

    @property
    def data_msg(self):
        return self._data_msg
    @data_msg.setter
    def data_msg(self, newvalue):
        self._data_msg = newvalue

    @property
    def is_data(self):
        if self._data_msg is not None:
            return True
        return False

    @property
    def is_proto(self):
        if self._proto_status is not None or self._proto_op is not None:
            return True
        return False

    @property
    def is_status(self):
        if self._proto_status is not None:
            return True
        return False

    @property
    def status(self):
        return self._proto_status
    @status.setter
    def status(self, newvalue):
        if newvalue not in ('ok', 'retry', 'failed', 'shutdown', 'quitserver'):
            raise ValueError('Incorrect value for status')
        self._proto_status = newvalue
        self.op = 'status' # automatically set it as status

    @property
    def op(self):
        return self._proto_op
    @op.setter
    def op(self, newvalue):
        if newvalue not in ('download', 'upload', 'status', 'params'):
            raise ValueError('Incorrect operation')
        self._proto_op = newvalue

    @property
    def proto_msg(self):
        return self._proto_msg
    @proto_msg.setter
    def proto_msg(self, newvalue):
        self._proto_msg = newvalue

    def compile(self):
        if self.is_proto and self.is_status:
            return json.dumps({'status': self.status})
        elif self.is_proto and not self.is_status:
            return json.dumps({'op': self.op, 'msg': self.proto_msg})

    def decompile(self, msg):
        payl = json.loads(msg)
        if 'status' in payl.keys():
            self.status = payl['status']
        else:
            self.op = payl['op']
            self.proto_msg = payl['msg']


#### Server classes ####
class TPFTServerConnection(TinyProtoConnection):
    def process_upload(self, p):
        upload_size = p.proto_msg['source_size']
        destination_path = p.proto_msg['destination']
        # let's see if we can write to the file
        try:
            destination_f = open(destination_path, 'wb')
        except IOError as e:
            payload = ProtoPayload()
            payload.status = 'failed'
            self.transmit(payload.compile())
            self.shutdown = True
        else:
            # report back this is ok, and await chunks in loop
            payload = ProtoPayload()
            payload.status = 'ok'
            self.transmit(payload.compile())
            total_count = 0
            while total_count < upload_size:
                upload_buffer = self.receive()
                destination_f.write(upload_buffer)
                total_count = total_count + len(upload_buffer)

                self.transmit(payload.compile())
            self.shutdown = True

    def process_download(self, p):
        # first lets see if it can open the source file for client download
        try:
            source_f = open(p.proto_msg['source'], 'rb')
        except IOError as e:
            # notify client it's not goint to happen
            payload = ProtoPayload()
            payload.status='failed'
            payload.proto_msg = 'Failed to open source file'
            self.transmit(payload.compile())
            self.shutdown = True
        else:
            # now let's see what this file is, and reply details to client
            f_stats = os.fstat(source_f.fileno())
            p.proto_msg['source_size'] = f_stats.st_size
            p.proto_msg['chunk_size'] = CHUNK_SIZE
            self.transmit(p.compile())

            # now let's check if client is ok with it
            response = self.receive()
            payload = ProtoPayload()
            payload.decompile(str(response))
            if payload.is_status and payload.status == 'failed':
                # probably client could not open a destination file for writing the download
                self.shutdown = True
            elif payload.is_status and payload.status == 'ok':
                total_count = 0
                while total_count < f_stats.st_size:
                    download_buffer = source_f.read(CHUNK_SIZE)
                    self.transmit(download_buffer)
                    response = self.receive()
                    payload = ProtoPayload()
                    payload.decompile(str(response))
                    if payload.status == 'ok':
                        total_count = total_count + len(download_buffer)
                    else:
                        # something went wrong
                        self.shutdown = True
                        return False

    def transmission_received(self, msg):
        try:
            payload = ProtoPayload()
            payload.decompile(str(msg))
        except Exception as e:
            # some weird message, quitting the connection
            payload = ProtoPayload()
            payload.status = 'shutdown'
            self.transmit(payload.compile())
            self.shutdown = True
        else:
            if payload.is_proto and payload.is_status and payload.status == 'shutdown':
                self.shutdown = True
            elif payload.is_proto and payload.op == 'upload':
                self.process_upload(payload)
            elif payload.is_proto and payload.op == 'download':
                self.process_download(payload)


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
    def upload_file(self, p):
        # first check if you can open the file to upload
        try:
            source_f = open(p.proto_msg['source'], 'rb')
        except IOError as e:
            self.shutdown = True
            return False

        # now that we have an opened file, we can test it's size and confirm with server
        f_stats = os.fstat(source_f.fileno())
        p.proto_msg['source_size'] = f_stats.st_size
        p.proto_msg['chunk_size'] = CHUNK_SIZE
        self.transmit(p.compile())

        # now awaiting confirmation from server
        response = self.receive()
        payload = ProtoPayload()
        payload.decompile(str(response))
        if payload.status == 'ok':
            total_count = 0
            while total_count < f_stats.st_size:
                upload_buffer = source_f.read(CHUNK_SIZE)
                self.transmit(upload_buffer)
                response = self.receive()
                payload = ProtoPayload()
                payload.decompile(str(response))
                if payload.status == 'ok':
                    total_count = total_count + len(upload_buffer)
                else:
                    # something went wrong
                    self.shutdown = True
                    return False
        else:
            self.shutdown = True

    def download_file(self, p):
        # first let's just pass on the info of wanting to download file and see what server responds
        self.transmit(p.compile())
        response = self.receive()
        payload = ProtoPayload()
        payload.decompile(str(response))
        if payload.is_status and (payload.status == 'failed' or payload.status == 'shutdown'):
            # probably server could not open the source file
            self.shutdown = True
            return False
        elif payload.op == 'download' and payload.proto_msg.has_key('source_size') and payload.proto_msg.has_key('chunk_size'):
            # this means server opened a file, nows it's size and will upload it in chunks
            # now we have to see if we can open a destination file and write to it
            download_size = payload.proto_msg['source_size']
            download_path = payload.proto_msg['destination']
            try:
                destination_f = open(download_path, 'wb')
            except IOError as e:
                payload = ProtoPayload()
                payload.status='failed'
                self.transmit(payload.compile())
                self.shutdown = True
            else:
                # report back all is ok and await chunks in loop
                payload = ProtoPayload()
                payload.status = 'ok'
                self.transmit(payload.compile())
                total_count = 0
                while total_count < download_size:
                    download_buffer = self.receive()
                    destination_f.write(download_buffer)
                    total_count = total_count + len(download_buffer)
                    self.transmit(payload.compile())
                self.shutdown = True

    def process_payload(self, p):
        if p.is_proto and p.op == 'upload':
            self.upload_file(p)
        elif p.is_proto and p.op == 'download':
            self.download_file(p)

    def loop_pass(self):
        for m in self.msg_from_parent():
            if m in ('shutdown', 'quitserver'):
                payload = ProtoPayload()
                payload.status = m
                self.transmit(payload.compile())
                self.shutdown = True
                break
            # self.transmit(m)
            # resp = self.receive()
            # self.msg_to_parent(resp)
            payload = ProtoPayload()
            payload.decompile(str(m))
            self.process_payload(payload)

    def transmission_received(self, msg):
        raise IOError('Unhandled transmission in client should not happen')


class TPFTClient(TinyProtoClient):
    __slots__ = TinyProtoServer.__slots__ + ('args',)

    def set_args(self, a):
        self.args = a

    def pre_loop(self):
        self.set_conn_handler(TPFTClientConnection)
        index = self.connect_to(self.args.host, self.args.port)
        if self.args.verbose:
            print('Starting connection ... ')

        payload = ProtoPayload()
        if self.args.download:
            payload.op = 'download'
        elif self.args.upload:
            payload.op = 'upload'
        payload.proto_msg = {'source': self.args.source, 'destination': self.args.destination}
        self.active_connections[index].msg_to_child(payload.compile())

    def loop_pass(self):
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
