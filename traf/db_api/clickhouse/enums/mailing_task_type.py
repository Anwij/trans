import enum


class MailingTaskType(enum.Enum):
    delete_message = enum.auto()
    accept_request = enum.auto()
    send_message = enum.auto()