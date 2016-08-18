#!/usr/bin/python
# -*- coding: utf-8 -*-

from functools import partial
from inspect import isroutine


def ppretty(obj, indent='    ', depth=4, width=120, seq_length=5, show_protected=False, show_private=False, show_static=False, show_properties=False, show_address=False):
    seq_formats = {list: ('[', ']'), tuple: ('(', ')'), set: ('set([', '])'), dict: ('{', '}')}

    def inspect_object(current_obj, current_depth, current_width):
        inspect_nested_object = partial(inspect_object, current_depth=current_depth - 1, current_width=current_width - len(indent))

        # Basic types
        if isinstance(current_obj, (int, long, float, basestring)):
            return [repr(current_obj)]

        # Class object
        if isinstance(current_obj, type):
            module = current_obj.__module__ + '.' if hasattr(current_obj, '__module__') else ''
            return ["<class '" + module + current_obj.__name__ + "'>"]

        # None
        if current_obj is None:
            return ['None']

        # Format block of lines
        def format_block(lines, open_bkt='', close_bkt=''):
            new_lines = []
            one_line = ''
            if open_bkt:
                new_lines.append(open_bkt)
                one_line += open_bkt
            for line in lines:
                new_lines.append(indent + line)
                if len(one_line) <= current_width:
                    one_line += line
            if close_bkt:
                if lines:
                    new_lines.append(close_bkt)
                else:
                    new_lines[-1] += close_bkt
                one_line += close_bkt

            return [one_line] if len(one_line) <= current_width and one_line else new_lines

        class SkipElement(object):
            pass

        class ErrorAttr(object):
            def __init__(self, e):
                self.e = e

        def cut_seq(seq):
            if current_depth < 1:
                return [SkipElement()]
            if len(seq) <= seq_length:
                return seq
            elif seq_length > 1:
                seq = list(seq) if isinstance(seq, tuple) else seq
                return seq[:seq_length / 2] + [SkipElement()] + seq[(1 - seq_length) / 2:]
            return [SkipElement()]

        def format_seq():
            r = []
            items = cut_seq(obj_items)
            for n, i in enumerate(items, 1):
                if type(i) is SkipElement:
                    r.append('...')
                else:
                    if type(current_obj) is dict:
                        (k, v) = i
                        k = inspect_nested_object(k)
                        v = inspect_nested_object(v)
                        k[-1] += ': ' + v.pop(0)
                        r.extend(k)
                        r.extend(format_block(v))
                    elif type(current_obj) in seq_formats:
                        r.extend(inspect_nested_object(i))
                    else:
                        (k, v) = i
                        k = [k]
                        v = inspect_nested_object(v) if type(v) is not ErrorAttr else ['<Error attribute: ' + type(v.e).__name__ + ': ' + v.e.message + '>']
                        k[-1] += ' = ' + v.pop(0)
                        r.extend(k)
                        r.extend(format_block(v))
                if n < len(items):
                    r[-1] += ', '
            return format_block(r, *brackets)

        # Sequence types
        # Others objects are considered as sequence of members
        if type(current_obj) in seq_formats:
            if type(current_obj) is dict:
                obj_items = current_obj.items()
            else:
                obj_items = current_obj
            brackets = seq_formats[type(current_obj)]
        else:
            obj_items = []
            for k in dir(current_obj):
                if not show_private and k.startswith('_') and '__' in k:
                    continue
                if not show_protected and k.startswith('_'):
                    continue
                try:
                    v = getattr(current_obj, k)
                    if isroutine(v):
                        continue
                    if not show_static and hasattr(type(current_obj), k) and v is getattr(type(current_obj), k):
                        continue
                    if not show_properties and hasattr(type(current_obj), k) and isinstance(
                            getattr(type(current_obj), k), property):
                        continue
                except Exception as e:
                    v = ErrorAttr(e)

                obj_items.append((k, v))

            module = current_obj.__module__ + '.' if hasattr(current_obj, '__module__') else ''
            address = ' at ' + hex(id(current_obj)) + ' ' if show_address else ''
            brackets = (module + type(current_obj).__name__ + address + '(', ')')

        return format_seq()

    return '\n'.join(inspect_object(obj, depth, width))


if __name__ == '__main__':
    class B(object):
        def __init__(self, b):
            self.b = b

    class A(object):
        i = [-3, 4.5, ('6', B({'\x07': 8}))]

        def __init__(self, a):
            self.a = a

    class C(object):
        def __init__(self):
            self.a = {u'1': A(2), '9': [10L, 11, {(12, 13): {14, None}}], 15: [16, 17, 18, 19, 20]}
            self.b = 'd'
            self._c = 'b'
            self.e = C.D

        d = 'c'

        def foo(self):
            pass

        @property
        def bar(self):
            return 'e'

        class D(object):
            pass


    print ppretty(C(), indent='    ', depth=8, width=41, seq_length=6, show_static=True, show_protected=True, show_properties=True, show_address=True)
    print ppretty(C(), depth=8, width=200, seq_length=4)
