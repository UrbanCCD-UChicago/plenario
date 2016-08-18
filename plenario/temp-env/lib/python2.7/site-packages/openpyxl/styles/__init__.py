from __future__ import absolute_import
# Copyright (c) 2010-2015 openpyxl

from openpyxl.descriptors import Typed

from .alignment import Alignment
from .borders import Border, Side
from .colors import Color
from .fills import PatternFill, GradientFill, Fill
from .fonts import Font
from .hashable import HashableObject
from .numbers import NumberFormatDescriptor, is_date_format, is_builtin
from .protection import Protection
from .proxy import StyleProxy


class Style(HashableObject):
    """Style object containing all formatting details."""
    __fields__ = ('font',
                  'fill',
                  'border',
                  'alignment',
                  'number_format',
                  'protection')
    __base__ = True

    _font = Typed(expected_type=Font)
    _fill = Typed(expected_type=Fill)
    _border = Typed(expected_type=Border)
    _alignment = Typed(expected_type=Alignment)
    number_format = NumberFormatDescriptor()
    _protection = Typed(expected_type=Protection)

    def __init__(self,
                 font=Font(),
                 fill=PatternFill(),
                 border=Border(),
                 alignment=Alignment(),
                 number_format=None,
                 protection=Protection()
                 ):
        self._font = font
        self._fill = fill
        self._border = border
        self._alignment = alignment
        self.number_format = number_format
        self._protection = protection


    @property
    def font(self):
        return StyleProxy(self._font)

    @property
    def fill(self):
        return StyleProxy(self._fill)

    @property
    def border(self):
        return StyleProxy(self._border)

    @property
    def alignment(self):
        return StyleProxy(self._alignment)

    @property
    def protection(self):
        return StyleProxy(self._protection)


DEFAULTS = Style()
