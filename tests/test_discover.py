from unittest import TestCase
import minimock
from pycassa.discover import Discover
from pycassa.pool import ConnectionPool
from tests import TEST_KS

__author__ = 'gilles'

class TestDiscover(TestCase):
    def setUp(self):
        super(TestDiscover, self).setUp()
        self.discover = Discover(TEST_KS)

    def test_get_ring(self):
        ring = self.discover.get_ring()
        self.assertEqual(ring, ['127.0.0.1:9160'])

    def test_pool_integration(self):
        tracker = minimock.TraceTracker()
        mocked_instance = minimock.Mock('Discover', tracker=tracker)
        mocked_instance.get_ring.mock_returns = ['abc:1234']
        mocked_class = minimock.Mock('Discover', tracker=tracker, returns=mocked_instance)

        pool = ConnectionPool('PycassaTestKeyspace', discover=mocked_class, prefill=False)

        self.assertEqual(pool.server_list, ['abc:1234'])
        minimock.assert_same_trace(tracker, '\n'.join([
            "Called Discover('PycassaTestKeyspace', ['localhost:9160'])",
            "Called Discover.get_ring()"
        ]))
