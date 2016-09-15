import unittest


class TestNodeMeta(unittest.TestCase):

    def test_nearest_neighbor_with_valid_node(self):

        from plenario.sensor_network.sensor_models import NodeMeta

        expected = "SENSOR_DEV_4"
        observed = NodeMeta.nearest_neighbor_to("SENSOR_DEV_1")

        self.assertEqual(expected, observed)
