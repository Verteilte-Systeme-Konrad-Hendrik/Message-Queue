import unittest
import node_message_store as nms


class TestNodeMessageStore(unittest.TestCase):
    def setUp(self):
        nms.init_db()

    def tearDown(self):
        nms.remove_db()

    def test_store_messages(self):
        # create mock messages
        pass