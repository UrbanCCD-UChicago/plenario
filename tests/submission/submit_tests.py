import unittest
from plenario.views import Submission


class EvilSubmitTests(unittest.TestCase):
    def test_nonsense_url(self):
        self.assertRaises(RuntimeError, lambda: Submission('totes Non$ense'))

    def test_hopeless_url(self):
        self.assertRaises(RuntimeError,
                          lambda: Submission('https://www.google.com/'))


class SubmitCSVTests(unittest.TestCase):
    def test_nonsense_url(self):
        pass

    def test_hopeless_url(self):
        pass

    def test_socrata_url(self):
        pass

    def test_non_socrata_url(self):
        pass


class SubmitShapeTests(unittest.TestCase):
    def test_nonsense_url(self):
        pass

    def test_hopeless_url(self):
        pass

    def test_socrata_url(self):
        pass

    def test_non_socrata_url(self):
        pass