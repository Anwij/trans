import enum


class TaskType(enum.Enum):
    send_message = enum.auto()
    delete_message = enum.auto()
    accept_join_request = enum.auto()