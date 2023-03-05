"""
Custom exceptions
"""
class WrongFormatException(Exception):
    """
    WrongFormatException class

    Exception raised when the format type give as a parameter
    is not supported
    """
class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class

    Exception raised when the meta file is wrong
    """