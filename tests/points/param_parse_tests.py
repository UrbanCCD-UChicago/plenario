import unittest
from plenario.api.point import ParamValidator, agg_validator, make_format_validator


class ParamParseTests(unittest.TestCase):
    def setUp(self):
        self.validator = ParamValidator().set_optional('agg', agg_validator, 'week')\
                                         .set_optional('data_type', make_format_validator(['json', 'csv']), 'json')

    def test_enum_defaults(self):
        params = {}
        err = self.validator.validate(params)
        self.assertIsNone(err)

        self.assertEqual(self.validator.vals['agg'], 'week')
        self.assertEqual(self.validator.vals['data_type'], 'json')

    def test_enum_valid(self):
        params = {'agg': 'day', 'data_type': 'csv'}
        err = self.validator.validate(params)
        self.assertIsNone(err)

        self.assertEqual(self.validator.vals['agg'], 'day')
        self.assertEqual(self.validator.vals['data_type'], 'csv')

    def test_enum_invalid(self):
        params = {'agg': 'millenium'}
        err = self.validator.validate(params)
        self.assertIsNotNone(err)
