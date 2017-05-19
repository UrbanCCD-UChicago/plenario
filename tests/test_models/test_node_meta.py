# FIXME(heyzoos)
# This test needs to rely on fixtures it creates for itself, not on the
# byproducts of other tests (which is what it used to do).

# from tests.fixtures.base_test import BasePlenarioTest
#
#
# class TestNodeMeta(BasePlenarioTest):
#
#     def test_nearest_neighbor_with_valid_node(self):
#
#         from plenario.models.SensorNetwork import NodeMeta
#
#         expected = "NODE_DEV_2"
#         observed = NodeMeta.nearest_neighbor_to("NODE_DEV_1")
#         self.assertEqual(expected, observed)
#
#         expected = "NODE_DEV_1"
#         observed = NodeMeta.nearest_neighbor_to("NODE_DEV_2")
#         self.assertEqual(expected, observed)
