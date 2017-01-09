import unittest


class TestHelpers(unittest.TestCase):

    def test_slugify(self):
        from plenario.utils.helpers import slugify
        self.assertEqual(slugify("A-Awef-Basdf-123"), "a_awef_basdf_123")
