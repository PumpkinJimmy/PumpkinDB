# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: pumpkindb.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='pumpkindb.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x0fpumpkindb.proto\" \n\x0cGreetRequest\x12\x10\n\x08\x63lientId\x18\x01 \x01(\t\",\n\rGreetResponse\x12\x0e\n\x06status\x18\x01 \x01(\t\x12\x0b\n\x03msg\x18\x02 \x01(\t\"l\n\nGetRequest\x12\x10\n\x08\x63lientId\x18\x01 \x01(\t\x12\x11\n\tcommandId\x18\x02 \x01(\x04\x12\x0b\n\x03key\x18\x03 \x01(\t\x12\x13\n\x0breceiptTerm\x18\x04 \x01(\r\x12\x17\n\x0freceiptLogIndex\x18\x05 \x01(\x04\">\n\rValueResponse\x12\r\n\x05value\x18\x01 \x01(\t\x12\x0c\n\x04term\x18\x02 \x01(\r\x12\x10\n\x08logIndex\x18\x03 \x01(\x04\"{\n\nPutRequest\x12\x10\n\x08\x63lientId\x18\x01 \x01(\t\x12\x11\n\tcommandId\x18\x02 \x01(\x04\x12\x0b\n\x03key\x18\x03 \x01(\t\x12\r\n\x05value\x18\x04 \x01(\t\x12\x13\n\x0breceiptTerm\x18\x05 \x01(\r\x12\x17\n\x0freceiptLogIndex\x18\x06 \x01(\x04\"b\n\x12RequestVoteRequest\x12\x0c\n\x04term\x18\x01 \x01(\r\x12\x13\n\x0b\x63\x61ndidateId\x18\x02 \x01(\t\x12\x14\n\x0clastLogIndex\x18\x03 \x01(\x04\x12\x13\n\x0blastLogTerm\x18\x04 \x01(\r\"8\n\x13RequestVoteResponse\x12\x0c\n\x04term\x18\x01 \x01(\r\x12\x13\n\x0bvoteGranted\x18\x02 \x01(\x08\"l\n\x05\x45ntry\x12\x10\n\x08\x63lientId\x18\x01 \x01(\t\x12\x11\n\tcommandId\x18\x02 \x01(\x04\x12\x11\n\toperation\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\t\x12\x0e\n\x06value1\x18\x05 \x01(\t\x12\x0e\n\x06value2\x18\x06 \x01(\t\"C\n\x08LogEntry\x12\x0c\n\x04term\x18\x01 \x01(\r\x12\x10\n\x08logIndex\x18\x02 \x01(\x04\x12\x17\n\x07\x63ommand\x18\x03 \x01(\x0b\x32\x06.Entry\"\x93\x01\n\x14\x41ppendEntriesRequest\x12\x0c\n\x04term\x18\x01 \x01(\r\x12\x10\n\x08leaderId\x18\x02 \x01(\t\x12\x14\n\x0cprevLogIndex\x18\x03 \x01(\x04\x12\x13\n\x0bprevLogTerm\x18\x04 \x01(\r\x12\x14\n\x0cleaderCommit\x18\x05 \x01(\x04\x12\x1a\n\x07\x65ntries\x18\x06 \x03(\x0b\x32\t.LogEntry\"6\n\x15\x41ppendEntriesResponse\x12\x0c\n\x04term\x18\x01 \x01(\r\x12\x0f\n\x07success\x18\x02 \x01(\x08\"\x89\x01\n\x06Status\x12\x0e\n\x06nodeId\x18\x07 \x01(\t\x12\x10\n\x08leaderId\x18\x01 \x01(\t\x12\x0f\n\x07voteFor\x18\x02 \x01(\t\x12\x0c\n\x04term\x18\x03 \x01(\r\x12\x13\n\x0b\x63ommitIndex\x18\x04 \x01(\x04\x12\x13\n\x0blastApplied\x18\x05 \x01(\x04\x12\x14\n\x0clastLogIndex\x18\x06 \x01(\x04\"\"\n\x0fMonitorResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x32\x81\x01\n\tPumpkinDB\x12(\n\x05Greet\x12\r.GreetRequest\x1a\x0e.GreetResponse\"\x00\x12$\n\x03Get\x12\x0b.GetRequest\x1a\x0e.ValueResponse\"\x00\x12$\n\x03Put\x12\x0b.PutRequest\x1a\x0e.ValueResponse\"\x00\x32\x80\x01\n\x04Raft\x12\x38\n\x0bRequestVote\x12\x13.RequestVoteRequest\x1a\x14.RequestVoteResponse\x12>\n\rAppendEntries\x12\x15.AppendEntriesRequest\x1a\x16.AppendEntriesResponse22\n\x07Monitor\x12\'\n\nSendStatus\x12\x07.Status\x1a\x10.MonitorResponseb\x06proto3'
)




_GREETREQUEST = _descriptor.Descriptor(
  name='GreetRequest',
  full_name='GreetRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='clientId', full_name='GreetRequest.clientId', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=19,
  serialized_end=51,
)


_GREETRESPONSE = _descriptor.Descriptor(
  name='GreetResponse',
  full_name='GreetResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='GreetResponse.status', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='msg', full_name='GreetResponse.msg', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=53,
  serialized_end=97,
)


_GETREQUEST = _descriptor.Descriptor(
  name='GetRequest',
  full_name='GetRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='clientId', full_name='GetRequest.clientId', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='commandId', full_name='GetRequest.commandId', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='key', full_name='GetRequest.key', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='receiptTerm', full_name='GetRequest.receiptTerm', index=3,
      number=4, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='receiptLogIndex', full_name='GetRequest.receiptLogIndex', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=99,
  serialized_end=207,
)


_VALUERESPONSE = _descriptor.Descriptor(
  name='ValueResponse',
  full_name='ValueResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='value', full_name='ValueResponse.value', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='term', full_name='ValueResponse.term', index=1,
      number=2, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='logIndex', full_name='ValueResponse.logIndex', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=209,
  serialized_end=271,
)


_PUTREQUEST = _descriptor.Descriptor(
  name='PutRequest',
  full_name='PutRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='clientId', full_name='PutRequest.clientId', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='commandId', full_name='PutRequest.commandId', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='key', full_name='PutRequest.key', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='PutRequest.value', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='receiptTerm', full_name='PutRequest.receiptTerm', index=4,
      number=5, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='receiptLogIndex', full_name='PutRequest.receiptLogIndex', index=5,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=273,
  serialized_end=396,
)


_REQUESTVOTEREQUEST = _descriptor.Descriptor(
  name='RequestVoteRequest',
  full_name='RequestVoteRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='term', full_name='RequestVoteRequest.term', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='candidateId', full_name='RequestVoteRequest.candidateId', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='lastLogIndex', full_name='RequestVoteRequest.lastLogIndex', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='lastLogTerm', full_name='RequestVoteRequest.lastLogTerm', index=3,
      number=4, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=398,
  serialized_end=496,
)


_REQUESTVOTERESPONSE = _descriptor.Descriptor(
  name='RequestVoteResponse',
  full_name='RequestVoteResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='term', full_name='RequestVoteResponse.term', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='voteGranted', full_name='RequestVoteResponse.voteGranted', index=1,
      number=2, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=498,
  serialized_end=554,
)


_ENTRY = _descriptor.Descriptor(
  name='Entry',
  full_name='Entry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='clientId', full_name='Entry.clientId', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='commandId', full_name='Entry.commandId', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='operation', full_name='Entry.operation', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='key', full_name='Entry.key', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value1', full_name='Entry.value1', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value2', full_name='Entry.value2', index=5,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=556,
  serialized_end=664,
)


_LOGENTRY = _descriptor.Descriptor(
  name='LogEntry',
  full_name='LogEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='term', full_name='LogEntry.term', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='logIndex', full_name='LogEntry.logIndex', index=1,
      number=2, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='command', full_name='LogEntry.command', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=666,
  serialized_end=733,
)


_APPENDENTRIESREQUEST = _descriptor.Descriptor(
  name='AppendEntriesRequest',
  full_name='AppendEntriesRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='term', full_name='AppendEntriesRequest.term', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='leaderId', full_name='AppendEntriesRequest.leaderId', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='prevLogIndex', full_name='AppendEntriesRequest.prevLogIndex', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='prevLogTerm', full_name='AppendEntriesRequest.prevLogTerm', index=3,
      number=4, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='leaderCommit', full_name='AppendEntriesRequest.leaderCommit', index=4,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='entries', full_name='AppendEntriesRequest.entries', index=5,
      number=6, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=736,
  serialized_end=883,
)


_APPENDENTRIESRESPONSE = _descriptor.Descriptor(
  name='AppendEntriesResponse',
  full_name='AppendEntriesResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='term', full_name='AppendEntriesResponse.term', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='success', full_name='AppendEntriesResponse.success', index=1,
      number=2, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=885,
  serialized_end=939,
)


_STATUS = _descriptor.Descriptor(
  name='Status',
  full_name='Status',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='nodeId', full_name='Status.nodeId', index=0,
      number=7, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='leaderId', full_name='Status.leaderId', index=1,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='voteFor', full_name='Status.voteFor', index=2,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='term', full_name='Status.term', index=3,
      number=3, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='commitIndex', full_name='Status.commitIndex', index=4,
      number=4, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='lastApplied', full_name='Status.lastApplied', index=5,
      number=5, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='lastLogIndex', full_name='Status.lastLogIndex', index=6,
      number=6, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=942,
  serialized_end=1079,
)


_MONITORRESPONSE = _descriptor.Descriptor(
  name='MonitorResponse',
  full_name='MonitorResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='success', full_name='MonitorResponse.success', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1081,
  serialized_end=1115,
)

_LOGENTRY.fields_by_name['command'].message_type = _ENTRY
_APPENDENTRIESREQUEST.fields_by_name['entries'].message_type = _LOGENTRY
DESCRIPTOR.message_types_by_name['GreetRequest'] = _GREETREQUEST
DESCRIPTOR.message_types_by_name['GreetResponse'] = _GREETRESPONSE
DESCRIPTOR.message_types_by_name['GetRequest'] = _GETREQUEST
DESCRIPTOR.message_types_by_name['ValueResponse'] = _VALUERESPONSE
DESCRIPTOR.message_types_by_name['PutRequest'] = _PUTREQUEST
DESCRIPTOR.message_types_by_name['RequestVoteRequest'] = _REQUESTVOTEREQUEST
DESCRIPTOR.message_types_by_name['RequestVoteResponse'] = _REQUESTVOTERESPONSE
DESCRIPTOR.message_types_by_name['Entry'] = _ENTRY
DESCRIPTOR.message_types_by_name['LogEntry'] = _LOGENTRY
DESCRIPTOR.message_types_by_name['AppendEntriesRequest'] = _APPENDENTRIESREQUEST
DESCRIPTOR.message_types_by_name['AppendEntriesResponse'] = _APPENDENTRIESRESPONSE
DESCRIPTOR.message_types_by_name['Status'] = _STATUS
DESCRIPTOR.message_types_by_name['MonitorResponse'] = _MONITORRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

GreetRequest = _reflection.GeneratedProtocolMessageType('GreetRequest', (_message.Message,), {
  'DESCRIPTOR' : _GREETREQUEST,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:GreetRequest)
  })
_sym_db.RegisterMessage(GreetRequest)

GreetResponse = _reflection.GeneratedProtocolMessageType('GreetResponse', (_message.Message,), {
  'DESCRIPTOR' : _GREETRESPONSE,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:GreetResponse)
  })
_sym_db.RegisterMessage(GreetResponse)

GetRequest = _reflection.GeneratedProtocolMessageType('GetRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETREQUEST,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:GetRequest)
  })
_sym_db.RegisterMessage(GetRequest)

ValueResponse = _reflection.GeneratedProtocolMessageType('ValueResponse', (_message.Message,), {
  'DESCRIPTOR' : _VALUERESPONSE,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:ValueResponse)
  })
_sym_db.RegisterMessage(ValueResponse)

PutRequest = _reflection.GeneratedProtocolMessageType('PutRequest', (_message.Message,), {
  'DESCRIPTOR' : _PUTREQUEST,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:PutRequest)
  })
_sym_db.RegisterMessage(PutRequest)

RequestVoteRequest = _reflection.GeneratedProtocolMessageType('RequestVoteRequest', (_message.Message,), {
  'DESCRIPTOR' : _REQUESTVOTEREQUEST,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:RequestVoteRequest)
  })
_sym_db.RegisterMessage(RequestVoteRequest)

RequestVoteResponse = _reflection.GeneratedProtocolMessageType('RequestVoteResponse', (_message.Message,), {
  'DESCRIPTOR' : _REQUESTVOTERESPONSE,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:RequestVoteResponse)
  })
_sym_db.RegisterMessage(RequestVoteResponse)

Entry = _reflection.GeneratedProtocolMessageType('Entry', (_message.Message,), {
  'DESCRIPTOR' : _ENTRY,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:Entry)
  })
_sym_db.RegisterMessage(Entry)

LogEntry = _reflection.GeneratedProtocolMessageType('LogEntry', (_message.Message,), {
  'DESCRIPTOR' : _LOGENTRY,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:LogEntry)
  })
_sym_db.RegisterMessage(LogEntry)

AppendEntriesRequest = _reflection.GeneratedProtocolMessageType('AppendEntriesRequest', (_message.Message,), {
  'DESCRIPTOR' : _APPENDENTRIESREQUEST,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:AppendEntriesRequest)
  })
_sym_db.RegisterMessage(AppendEntriesRequest)

AppendEntriesResponse = _reflection.GeneratedProtocolMessageType('AppendEntriesResponse', (_message.Message,), {
  'DESCRIPTOR' : _APPENDENTRIESRESPONSE,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:AppendEntriesResponse)
  })
_sym_db.RegisterMessage(AppendEntriesResponse)

Status = _reflection.GeneratedProtocolMessageType('Status', (_message.Message,), {
  'DESCRIPTOR' : _STATUS,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:Status)
  })
_sym_db.RegisterMessage(Status)

MonitorResponse = _reflection.GeneratedProtocolMessageType('MonitorResponse', (_message.Message,), {
  'DESCRIPTOR' : _MONITORRESPONSE,
  '__module__' : 'pumpkindb_pb2'
  # @@protoc_insertion_point(class_scope:MonitorResponse)
  })
_sym_db.RegisterMessage(MonitorResponse)



_PUMPKINDB = _descriptor.ServiceDescriptor(
  name='PumpkinDB',
  full_name='PumpkinDB',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=1118,
  serialized_end=1247,
  methods=[
  _descriptor.MethodDescriptor(
    name='Greet',
    full_name='PumpkinDB.Greet',
    index=0,
    containing_service=None,
    input_type=_GREETREQUEST,
    output_type=_GREETRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='Get',
    full_name='PumpkinDB.Get',
    index=1,
    containing_service=None,
    input_type=_GETREQUEST,
    output_type=_VALUERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='Put',
    full_name='PumpkinDB.Put',
    index=2,
    containing_service=None,
    input_type=_PUTREQUEST,
    output_type=_VALUERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_PUMPKINDB)

DESCRIPTOR.services_by_name['PumpkinDB'] = _PUMPKINDB


_RAFT = _descriptor.ServiceDescriptor(
  name='Raft',
  full_name='Raft',
  file=DESCRIPTOR,
  index=1,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=1250,
  serialized_end=1378,
  methods=[
  _descriptor.MethodDescriptor(
    name='RequestVote',
    full_name='Raft.RequestVote',
    index=0,
    containing_service=None,
    input_type=_REQUESTVOTEREQUEST,
    output_type=_REQUESTVOTERESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='AppendEntries',
    full_name='Raft.AppendEntries',
    index=1,
    containing_service=None,
    input_type=_APPENDENTRIESREQUEST,
    output_type=_APPENDENTRIESRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_RAFT)

DESCRIPTOR.services_by_name['Raft'] = _RAFT


_MONITOR = _descriptor.ServiceDescriptor(
  name='Monitor',
  full_name='Monitor',
  file=DESCRIPTOR,
  index=2,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=1380,
  serialized_end=1430,
  methods=[
  _descriptor.MethodDescriptor(
    name='SendStatus',
    full_name='Monitor.SendStatus',
    index=0,
    containing_service=None,
    input_type=_STATUS,
    output_type=_MONITORRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_MONITOR)

DESCRIPTOR.services_by_name['Monitor'] = _MONITOR

# @@protoc_insertion_point(module_scope)
