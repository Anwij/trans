import enum


class CHMessageType(enum.Enum):
    greeting = enum.auto()
    greeting_2 = enum.auto()
    goodbye = enum.auto()
    start_message = enum.auto()
    message_after_joining = enum.auto()
    block_message = enum.auto()