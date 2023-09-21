import enum


class ActionType(enum.Enum):
    accepted = enum.auto()
    declined = enum.auto()
    join_declined = enum.auto()
    saved = enum.auto()