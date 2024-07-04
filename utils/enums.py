from enum import Enum

class LiveUpdateTarget(Enum):
    SPECIFIC_USER = 1
    ALL_USER = 2
    INACTIVE_USER = 3

class LiveUpdateType(Enum):
    GENERAL = 1
    CCTV = 2
    PROMOTION = 3