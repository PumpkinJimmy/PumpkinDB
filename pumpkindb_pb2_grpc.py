# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import pumpkindb_pb2 as pumpkindb__pb2


class PumpkinDBStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Greet = channel.unary_unary(
                '/PumpkinDB/Greet',
                request_serializer=pumpkindb__pb2.GreetRequest.SerializeToString,
                response_deserializer=pumpkindb__pb2.GreetResponse.FromString,
                )
        self.Get = channel.unary_unary(
                '/PumpkinDB/Get',
                request_serializer=pumpkindb__pb2.GetRequest.SerializeToString,
                response_deserializer=pumpkindb__pb2.ValueResponse.FromString,
                )
        self.Put = channel.unary_unary(
                '/PumpkinDB/Put',
                request_serializer=pumpkindb__pb2.PutRequest.SerializeToString,
                response_deserializer=pumpkindb__pb2.ValueResponse.FromString,
                )


class PumpkinDBServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Greet(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Get(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Put(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_PumpkinDBServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Greet': grpc.unary_unary_rpc_method_handler(
                    servicer.Greet,
                    request_deserializer=pumpkindb__pb2.GreetRequest.FromString,
                    response_serializer=pumpkindb__pb2.GreetResponse.SerializeToString,
            ),
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=pumpkindb__pb2.GetRequest.FromString,
                    response_serializer=pumpkindb__pb2.ValueResponse.SerializeToString,
            ),
            'Put': grpc.unary_unary_rpc_method_handler(
                    servicer.Put,
                    request_deserializer=pumpkindb__pb2.PutRequest.FromString,
                    response_serializer=pumpkindb__pb2.ValueResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'PumpkinDB', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class PumpkinDB(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Greet(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/PumpkinDB/Greet',
            pumpkindb__pb2.GreetRequest.SerializeToString,
            pumpkindb__pb2.GreetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Get(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/PumpkinDB/Get',
            pumpkindb__pb2.GetRequest.SerializeToString,
            pumpkindb__pb2.ValueResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Put(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/PumpkinDB/Put',
            pumpkindb__pb2.PutRequest.SerializeToString,
            pumpkindb__pb2.ValueResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
