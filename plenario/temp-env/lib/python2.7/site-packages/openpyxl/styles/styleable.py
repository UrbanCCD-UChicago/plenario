from __future__ import absolute_import
# Copyright (c) 2010-2015 openpyxl

from collections import namedtuple

from openpyxl.utils.indexed_list import IndexedList
from .numbers import BUILTIN_FORMATS, BUILTIN_FORMATS_REVERSE
from .proxy import StyleProxy
from . import Style


StyleId = namedtuple("StyleId", "alignment border fill font number_format protection")


class StyleDescriptor(object):

    def __init__(self, collection, key):
        self.collection = collection
        self.key = key

    def __set__(self, instance, value):
        coll = getattr(instance.parent.parent, self.collection)
        setattr(instance, self.key, coll.add(value))


    def __get__(self, instance, cls):
        coll = getattr(instance.parent.parent, self.collection)
        idx = getattr(instance, self.key)
        return StyleProxy(coll[idx])


class NumberFormatDescriptor(object):

    key = '_number_format_id'
    collection = '_number_formats'

    def __set__(self, instance, value):
        coll = getattr(instance.parent.parent, self.collection)
        if value in BUILTIN_FORMATS_REVERSE:
            _id = BUILTIN_FORMATS_REVERSE[value]
        else:
            _id = coll.add(value) + 164
        setattr(instance, self.key, _id)


    def __get__(self, instance, cls):
        idx = getattr(instance, self.key)
        if idx < 164:
            return BUILTIN_FORMATS.get(idx, "General")
        coll = getattr(instance.parent.parent, self.collection)
        return coll[idx - 164]


class StyleableObject(object):
    """
    Base class for styleble objects implementing proxy and lookup functions
    """

    font = StyleDescriptor('_fonts', '_font_id')
    fill = StyleDescriptor('_fills', '_fill_id')
    border = StyleDescriptor('_borders', '_border_id')
    number_format = NumberFormatDescriptor()
    protection = StyleDescriptor('_protections', '_protection_id')
    alignment = StyleDescriptor('_alignments', '_alignment_id')

    __slots__ = ('parent', '_font_id', '_border_id', '_fill_id',
                 '_alignment_id', '_protection_id', '_number_format_id')

    def __init__(self, sheet, font=0, fill=0, border=0, alignment=0, protection=0, number_format=0):
        self._font_id = font
        self._fill_id = fill
        self._border_id = border
        self._alignment_id = alignment
        self._protection_id = protection
        self._number_format_id = number_format
        self.parent = sheet


    @property
    def style_id(self):
        style = StyleId(self._alignment_id,
                        self._border_id,
                        self._fill_id,
                        self._font_id,
                        self._number_format_id,
                        self._protection_id)
        return self.parent.parent._cell_styles.add(style)

    @property
    def has_style(self):
        return bool(self._alignment_id
               or self._border_id
               or self._fill_id
               or self._font_id
               or self._number_format_id
               or self._protection_id)

    #legacy
    @property
    def style(self):
        return Style(
            font=self.font,
            fill=self.fill,
            border=self.border,
            alignment=self.alignment,
            number_format=self.number_format,
            protection=self.protection
        )

    #legacy
    @style.setter
    def style(self, value):
        self.font = value.font._StyleProxy__target
        self.fill = value.fill._StyleProxy__target
        self.border = value.border._StyleProxy__target
        self.protection = value.protection._StyleProxy__target
        self.alignment = value.alignment._StyleProxy__target
        self.number_format = value.number_format
