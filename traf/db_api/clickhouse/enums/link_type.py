import enum


class LinkType(enum.Enum):
    public_channel = enum.auto()
    private_channel = enum.auto()
    channel_with_join_request = enum.auto()
    public_group = enum.auto()
    private_group = enum.auto()
    group_with_join_request = enum.auto()
    bot = enum.auto()
    bot_referral = enum.auto()
    forwarded = enum.auto()