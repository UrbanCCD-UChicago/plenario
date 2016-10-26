import unittest


class TestNodeMeta(unittest.TestCase):

    def test_nearest_neighbor_with_valid_node(self):

        from plenario.models.SensorNetwork import NodeMeta

        expected = "NODE_DEV_2"
        observed = NodeMeta.nearest_neighbor_to("NODE_DEV_1")
        self.assertEqual(expected, observed)

        expected = "NODE_DEV_1"
        observed = NodeMeta.nearest_neighbor_to("NODE_DEV_2")
        self.assertEqual(expected, observed)

