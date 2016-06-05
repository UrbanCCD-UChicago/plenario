from unittest import TestCase
from api.validators import validators


class TestValidators(TestCase):

    def test_dataset_validator_good_table(self):

        self.assertTrue(validators['dataset']('flu_shot_clinics'))

    def test_dataset_validator_bad_table(self):

        self.assertFalse(validators['dataset']('bad_table_name'))

    def test_agg_validator_good_agg(self):

        self.assertTrue(validators['agg']('day'))

    def test_agg_validator_bad_agg(self):

        self.assertFalse(validators['agg']('wat'))
