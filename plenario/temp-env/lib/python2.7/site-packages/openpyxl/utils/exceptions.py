from __future__ import absolute_import
# Copyright (c) 2010-2015 openpyxl


"""Definitions for openpyxl shared exception classes."""


class CellCoordinatesException(Exception):
    """Error for converting between numeric and A1-style cell references."""

class IllegalCharacterError(Exception):
    """The data submitted which cannot be used directly in Excel files. It
    must be removed or escaped."""

class ColumnStringIndexException(Exception):
    """Error for bad column names in A1-style cell references."""

class DataTypeException(Exception):
    """Error for any data type inconsistencies."""

class NamedRangeException(Exception):
    """Error for badly formatted named ranges."""

class SheetTitleException(Exception):
    """Error for bad sheet names."""

class InsufficientCoordinatesException(Exception):
    """Error for partially specified cell coordinates."""

class OpenModeError(Exception):
    """Error for fileobj opened in non-binary mode."""

class InvalidFileException(Exception):
    """Error for trying to open a non-ooxml file."""

class ReadOnlyWorkbookException(Exception):
    """Error for trying to modify a read-only workbook"""

class MissingNumberFormat(Exception):
    """Error when a referenced number format is not in the stylesheet"""

class WorkbookAlreadySaved(Exception):
    """Error when attempting to perform operations on a dump workbook
    while it has already been dumped once"""
