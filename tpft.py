#!/usr/bin/env python3
#tpft - tiny proto file transfer
import os
from tinyproto import TinyProtoServer, TinyProtoClient, TinyProtoConnection, TinyProtoConnectionDetails
import argparse
import json
import time

DEFAULT_CHUNK_SIZE=4 * 1024 * 1024
DEFAULT_PORT=8088

class RemoteHostInvalidError(Exception):
    pass

class RemotePathInvalidError(Exception):
    pass

class LocalPathFileDoesNotExistError(Exception):
    pass

class CommunicationDecodeError(Exception):
    pass

class InsufficientPathsProvidedError(Exception):
    pass

class MultipleUploadsUnsupportedError(Exception):
    pass

class MultipleRemotePathsError(Exception):
    pass

class NoRemotePathError(Exception):
    pass

arg_parser = argparse.ArgumentParser('Client/Server file transfer tool.')
arg_parser.add_argument('-l', '--listen', action='store', type=str, help='Start listener server instead of uploading/downloading a file')
arg_parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Enable verbosity. UNIMPLEMENTED')
arg_parser.add_argument('-p', '--progress', action='store_true', default=False, help='Display progress information. UNIMPLEMENTED')
arg_parser.add_argument('--chunk-size', action='store', type=int, default=DEFAULT_CHUNK_SIZE, help='Default chunk size. File will be split into chunks for transfer. Default size {}MB'.format(DEFAULT_CHUNK_SIZE/1024/1024))
arg_parser.add_argument('path', action='store', type=str, nargs='*', help='Source and destination file paths. There can be multiple local paths, but only one remote path')


class Letter:
    __slots__ = ('_container', )
    _type_ = 'base-letter'

    def __init__(self, payload = None):
        self._container = {}
        if payload is not None:
            self.load_payload(payload)

    def load_payload(self, payload):
        self._container = payload

    def dump_payload(self):
        return self._container

class Envelope:
    __slots__ = ('letter',)

    def __init__(self, payload):
        if isinstance(payload, Letter):
            self.load_from_payload(payload)
        else:
            self.load_from_json(payload)

    def load_from_payload(self, payload):
        self.letter = payload

    def load_from_json(self, payload):
        try:
            decoded = json.loads(payload)
        except Exception as e:
            print(payload)
            raise e
        if not isinstance(decoded, dict) or 'type' not in decoded or 'letter' not in decoded:
            raise CommunicationDecodeError(payload)

        if decoded['type'] == ConfirmationLetter._type_:
            self.letter = ConfirmationLetter(decoded['letter'])
        elif decoded['type'] == RejectionLetter._type_:
            self.letter = RejectionLetter(decoded['letter'])
        elif decoded['type'] == ConnectionCloseLetter._type_:
            self.letter = ConnectionCloseLetter(decoded['letter'])
        elif decoded['type'] == UploadRequestLetter._type_:
            self.letter = UploadRequestLetter(decoded['letter'])
        elif decoded['type'] == DownloadRequestLetter._type_:
            self.letter = DownloadRequestLetter(decoded['letter'])
        elif decoded['type'] == DownloadConfirmationLetter._type_:
            self.letter = DownloadConfirmationLetter(decoded['letter'])
        else:
            self.letter = Letter(decoded['letter'])

    def dump(self):
        return json.dumps({
            'type': self.letter.__class__._type_,
            'letter': self.letter.dump_payload()
        }, separators=(',', ':'))

    def __str__(self):
        return self.dump()

class ConfirmationLetter(Letter):
    _type_ = 'confirmed'

class DownloadConfirmationLetter(ConfirmationLetter):
    _type_ = 'download-confirmation'

    @property
    def FileSize(self):
        return self._container.get('file_size')
    @FileSize.setter
    def FileSize(self, newvalue):
        if not isinstance(newvalue, int):
            raise ValueError('File size can only be integer value in bytes')
        self._container['file_size'] = newvalue

class RejectionLetter(Letter):
    _type_ = 'rejected'

    @property
    def Reason(self):
        return self._container.get('reason')
    @Reason.setter
    def Reason(self, newvalue):
        self._container['reason'] = newvalue

class ConnectionCloseLetter(Letter):
    _type_ = 'close'

class UploadRequestLetter(Letter):
    _type_ = 'upload-request'

    @property
    def FileSize(self):
        return self._container.get('file_size')
    @FileSize.setter
    def FileSize(self, newvalue):
        if not isinstance(newvalue, int):
            raise ValueError('File size must be an integer number of bytes')
        self._container['file_size'] = newvalue

    @property
    def DestinationPath(self):
        return self._container.get('destination_path')
    @DestinationPath.setter
    def DestinationPath(self, newvalue):
        self._container['destination_path'] = newvalue

class DownloadRequestLetter(Letter):
    _type_ = 'download-request'

    @property
    def DownloadPath(self):
        return self._container.get('download_path')
    @DownloadPath.setter
    def DownloadPath(self, newvalue):
        self._container['download_path'] = newvalue




class TpftServerConnection(TinyProtoConnection):
    __slots__ = ('verbose', 'display_progress', 'uuid')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.verbose = False
        self.display_progress = False
        self.uuid = None

    def handle_upload(self, file_size, destination_path):
        try:
            fd = open(destination_path, 'wb')
        except OSError as e:
            l = RejectionLetter()
            l.Reason = str(e)
            self.transmit(str(Envelope(l)).encode())
            if self.verbose:
                print("[{}] Failed to open destination path: {} - Sending reject.".format(self.uuid, str(e)))
        else:
            self.transmit(str(Envelope(ConfirmationLetter())).encode())

            if self.verbose:
                print("[{}] Destination file opened. Confirmation sent. Starting data transfer ... ".format(self.uuid))
            count = 0
            while count < file_size:
                buff = self.receive()
                count = count + len(buff)
                fd.write(buff)
            if self.verbose:
                print("[{}] Transfer complete. Saving to disk ... ".format(self.uuid))
            fd.close()
            if self.verbose:
                print("[{}] File saved. Sending confirmation to client.".format(self.uuid))
            self.transmit(str(Envelope(ConfirmationLetter())).encode())

    def handle_download(self, download_path):
        try:
            file_size = os.path.getsize(download_path)
        except OSError as e:
            l = RejectionLetter()
            l.Reason = str(e)
            self.transmit(str(Envelope(l)).encode())
            if self.verbose:
                print("[{}] Failed to open download path: {} - Sending reject.".format(self.uuid, str(e)))
        else:
            fd = open(download_path, 'rb')
            if self.verbose:
                print("[{}] Opened file for reading. Sending download confirmation ... ".format(self.uuid))

            l = DownloadConfirmationLetter()
            l.FileSize = file_size
            self.transmit(str(Envelope(l)).encode())

            msg = self.receive()
            envelope = Envelope(msg)
            if not isinstance(envelope.letter, ConfirmationLetter):
                if self.verbose:
                    print("[{}] Client rejected file.".format(self.uuid))
            else:
                if self.verbose:
                    print("[{}] Client accepted file. Starting transfer ... ".format(self.uuid))

                count = 0
                while count < file_size:
                    buff = fd.read(DEFAULT_CHUNK_SIZE)
                    self.transmit(buff)
                    count = count + len(buff)

                msg = self.receive()
                envelope = Envelope(msg)
                if not isinstance(envelope.letter, ConfirmationLetter) and self.verbose:
                    print("[{}] Client rejected binary transfer.".format(self.uuid))
                if self.verbose:
                    print("[{}] Transfer completed.".format(self.uuid))
            fd.close()

    def handle_message(self, msg):
        if not isinstance(msg, Envelope):
            raise ValueError('handle_letter only accepts instances of Letter class')

        letter = msg.letter
        if isinstance(letter, ConnectionCloseLetter):
            self.shutdown = True
        elif isinstance(letter, UploadRequestLetter):
            if self.verbose:
                print("[{}] Requested upload of file {} of size {}".format(self.uuid, letter.DestinationPath, letter.FileSize))
            self.handle_upload(letter.FileSize, letter.DestinationPath)
        elif isinstance(letter, DownloadRequestLetter):
            if self.verbose:
                print("[{}] Requested download of file {}".format(self.uuid, letter.DownloadPath))
            self.handle_download(letter.DownloadPath)
        else:
            if self.verbose:
                print("[{}] Unhandleable letter received {}".format(self.uuid, letter.__class__))

    def transmission_received(self, msg):
        envelope = Envelope(msg)
        self.handle_message(envelope)

class TpftServer(TinyProtoServer):
    __slots__ = ('verbose', 'display_progress')

    def conn_init(self, conn_id, conn_o):
        if self.verbose:
            print('[SRV] Opened connection from {}'.format(conn_o.socket_o.getpeername()))
        conn_o.verbose = self.verbose
        conn_o.display_progress = self.display_progress
        conn_o.uuid = conn_id

    def conn_shutdown(self, conn_id, conn_o):
        if self.verbose:
            print('[SRV] Connection closed from {}'.format(conn_o.peername_details))



class TpftClientUploadConnection(TinyProtoConnection):
    __slots__ = ('source_file_descriptor', 'source_file_size', 'destination_path', 'ready_for_upload', 'progress')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ready_for_upload = False

    def upload_file(self, source_file, source_file_size, destination_path):
        self.source_file_descriptor = source_file
        self.source_file_size = source_file_size
        self.destination_path = destination_path
        self.ready_for_upload = True
        self.progress = 0

    def request_upload(self):
        l = UploadRequestLetter()
        l.FileSize = self.source_file_size
        l.DestinationPath = self.destination_path
        msg = Envelope(l)
        self.transmit(str(msg).encode())

        response = self.receive()
        return Envelope(response)

    def upload_confirmed(self):
        msg = self.receive()
        envelope = Envelope(msg)
        if isinstance(envelope.letter, ConfirmationLetter):
            return True
        else:
            return False

    def upload_binary(self):
        count = 0
        while count < self.source_file_size:
            buff = self.source_file_descriptor.read(DEFAULT_CHUNK_SIZE)
            self.transmit(buff)
            count = count + len(buff)
            self.progress = int(count / self.source_file_size * 100)

    def loop_pass(self):
        if self.ready_for_upload:
            msg = self.request_upload()

            if isinstance(msg.letter, ConfirmationLetter):
                self.upload_binary()
                msg = self.upload_confirmed()
            elif isinstance(msg.letter, RejectionLetter):
                print(msg.letter.Reason)

            self.transmit(str(Envelope(ConnectionCloseLetter())).encode())
            time.sleep(0.1)
            self.shutdown = True

    def pre_loop(self):
        self.socket_o.settimeout(90)

class TpftClientDownloadConnection(TinyProtoConnection):
    __slots__ = ('local_path', 'remote_path', 'ready_for_download', 'progress')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ready_for_download = False

    def download_file(self, remote_path, local_path):
        self.local_path = local_path
        self.remote_path = remote_path
        self.ready_for_download = True
        self.progress = 0

    def request_download(self):
        l = DownloadRequestLetter()
        l.DownloadPath = self.remote_path
        msg = Envelope(l)
        self.transmit(str(msg).encode())

        response = self.receive()
        return Envelope(response)

    def download_binary(self, file_size):
        count = 0
        fd = open(self.local_path, 'wb')
        while count < file_size:
            buff = self.receive()
            fd.write(buff)
            count = count + len(buff)
            self.progress = int(count / file_size * 100)
        fd.close()

    def accept_download(self):
        self.transmit(str(Envelope(ConfirmationLetter())).encode())

    def confirm_download(self):
        self.transmit(str(Envelope(ConfirmationLetter())).encode())

    def loop_pass(self):
        if self.ready_for_download:
            msg = self.request_download()

            if isinstance(msg.letter, DownloadConfirmationLetter):
                self.accept_download()
                self.download_binary(msg.letter.FileSize)
                self.confirm_download()
            elif isinstance(msg.letter, RejectionLetter):
                print(msg.letter.Reason)

            self.transmit(str(Envelope(ConnectionCloseLetter())).encode())
            time.sleep(0.1)
            self.shutdown = True

    def pre_loop(self):
        self.socket_o.settimeout(90)


class TpftClient(TinyProtoClient):
    def upload_file(self, local_path, remote_path, progress):
        self.set_conn_handler(TpftClientUploadConnection)
        if not isinstance(local_path, ParsedPath) or not isinstance(remote_path, ParsedPath):
            raise ValueError('Paths need to be instances of ParsedPath')

        connection_details = TinyProtoConnectionDetails(remote_path.host, remote_path.port if remote_path.port is not None else DEFAULT_PORT)
        uuid = self.connect_to(connection_details)
        connection = self.active_connections[uuid]
        connection.upload_file(local_path.filedescriptor, local_path.filesize, remote_path.path)

        while connection.is_alive():
            if progress:
                print('\rProgress: {}%'.format(connection.progress), end='')
            time.sleep(0.1)
        print()

    def download_file(self, remote_path, local_path, progress):
        self.set_conn_handler(TpftClientDownloadConnection)
        if not isinstance(local_path, ParsedPath) or not isinstance(remote_path, ParsedPath):
            raise ValueError('Paths need to be instances of ParsedPath')

        connection_details = TinyProtoConnectionDetails(remote_path.host, remote_path.port if remote_path.port is not None else DEFAULT_PORT)
        uuid = self.connect_to(connection_details)
        connection = self.active_connections[uuid]
        connection.download_file(remote_path.path, local_path.path)

        while connection.is_alive():
            if progress:
                print('\rProgress: {}%'.format(connection.progress), end='')
            time.sleep(0.1)
        print()


class ParsedPath:
    __slots__ = ('raw_path', 'path', 'directory', 'filename', 'filesize', 'filedescriptor', 'fileexists', 'is_remote', 'host', 'port')

    def __init__(self, raw_path):
        self.raw_path = raw_path
        self.path = None
        self.directory = None
        self.filename = None
        self.filesize = None
        self.filedescriptor = None
        self.is_remote = False
        self.host = None
        self.port = None

        self.parse_raw_path()

    def parse_raw_path(self):
        components = self.raw_path.split(':')
        if len(components) > 1:
            self.is_remote = True
            self.parse_remote_path(components)
        else:
            self.parse_local_path()

    def parse_remote_path(self, components):
        if len(components) > 3:
            raise RemotePathInvalidError(self.raw_path)
        elif len(components) == 2:
            host, path = components[0], components[1]
            port = None
        elif len(components) == 3:
            try:
                host, port, path = components[0], int(components[1]), components[2]
            except ValueError:
                raise RemotePathInvalidError(self.raw_path)

        filename = os.path.basename(path)
        directory = os.path.dirname(path)

        self.path = path
        self.filename = filename
        self.directory = directory
        self.host = host
        self.port = port

    def parse_local_path(self):
        self.path = self.raw_path
        self.filename = os.path.basename(self.path)
        self.directory = os.path.dirname(self.path)

        if not os.path.isfile(self.path):
            self.fileexists = False
        else:
            self.fileexists = True
            self.filesize = os.path.getsize(self.path)
            self.filedescriptor = open(self.path, 'rb')




def parse_path_set(paths):
    return [ParsedPath(p) for p in paths]

def get_host_port(remote_str):
    components = remote_str.split(':')
    if len(components) == 0 or len(components) > 2:
        raise RemoteHostInvalidError(remote_str)
    elif len(components) == 2:
        try:
            port = int(components[1])
        except ValueError:
            raise RemoteHostInvalidError(remote_str)
        return components[0], port
    else:
        return remote_str, DEFAULT_PORT

def handle_server(args):
    listen_host, listen_port = get_host_port(args.listen)
    if args.verbose:
        print('Picked server initiation on host {} port {}'.format(listen_host, listen_port))

    srv_connection_details = TinyProtoConnectionDetails(listen_host, listen_port)
    srv = TpftServer(listen_addresses = [srv_connection_details], connection_handler = TpftServerConnection)
    srv.verbose = args.verbose
    srv.display_progress = args.progress
    srv.start()

def handle_client(args):
    parsed_paths = parse_path_set(args.path)

    if len(parsed_paths) <= 1:
        raise InsufficientPathsProvidedError()
    elif len(parsed_paths) > 2 and not parsed_paths[0].is_remote:
        raise MultipleUploadsUnsupportedError()
    elif len([True for p in parsed_paths if p.is_remote]) > 1:
        raise MultipleRemotePathsError()
    elif len([True for p in parsed_paths if p.is_remote]) < 1:
        raise NoRemotePathError()
    elif len(parsed_paths) == 2 and parsed_paths[1].is_remote:
        if not parsed_paths[0].fileexists:
            raise LocalPathFileDoesNotExistError(parsed_paths[0].path)
        handle_client_upload(parsed_paths, args.progress)
    else:
        handle_client_download(parsed_paths, args.progress)

def handle_client_upload(parsed_paths, progress = False):
    local_path, remote_path = parsed_paths
    client = TpftClient()
    client.upload_file(local_path, remote_path, progress)

def handle_client_download(parsed_paths, progress):
    remote_path, local_path = parsed_paths
    client = TpftClient()
    client.download_file(remote_path, local_path, progress)


if __name__ == '__main__':
    args = arg_parser.parse_args()
    try:
        if args.listen is None:
            handle_client(args)
        else:
            handle_server(args)
    except RemoteHostInvalidError as e:
        print('Remote string {} invalid. Please specify remote host in form of host[:port]. Port must be an integer'.format(e.args[0]))
    except RemotePathInvalidError as e:
        print('Remote string {} invalid. Please specify remote host in form of host[:port]:/path/to/file. Port must be an integer'.format(e.args[0]))
    except LocalPathFileDoesNotExistError as e:
        print('Path {} does not exist or is not a file'.format(e.args[0]))
    except InsufficientPathsProvidedError as e:
        print('At least a source and a destination needs to be provided')
    except MultipleUploadsUnsupportedError as e:
        print('Multiple local uploads is not supported')
    except MultipleRemotePathsError as e:
        print('Multiple remote paths is not supported')
    except NoRemotePathError as e:
        print('One of the paths needs to be a remote path')
    except Exception as e:
        print('Unhandled exception! PANIC!')
        raise e
