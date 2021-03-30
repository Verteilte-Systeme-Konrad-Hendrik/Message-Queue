import unittest
import node_message_store as nms
import node_message as nmsg
import uuid

# To run tests in specific order add alphanumeric order

class TestNodeMessageStore(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        nms.init_db()
        
        cls.messages = []

        test_uuid = uuid.uuid4()
        test_index = 1

        # min value of 2
        cls.duplicates = 2
        assert(cls.duplicates >=2)

        # generate duplicates to test ignore behavior
        for i in range(5):
            if i < cls.duplicates:
                cls.messages.append(nmsg.make_queue_message(test_uuid, test_index, ("This is test {}".format(str(test_index))).encode("UTF-8")))
            else:
                cls.messages.append(nmsg.make_queue_message(uuid.uuid4(), i+1, ("This is test {}".format(str(i+1))).encode("UTF-8")))

        cls.pool_unconf = uuid.uuid4()

    @classmethod
    def tearDownClass(cls):
        nms.remove_db()


    def test_a_store_messages(self):
        nms.store_messages(self.messages)

    
    def test_b_store_messages_in_pool(self):
        nms.store_messages_in_pool(self.messages, self.pool_unconf)


    def test_c_get_message_for_pool(self):
        received_msg = nms.get_message_for_pool(self.pool_unconf)

        for i in range(1, self.duplicates):
            del self.messages[i]

        # print("Received:")
        # print(received_msg)

        # print("Compare:")
        # print(self.messages)
        self.assertSequenceEqual(self.messages, nms.get_message_for_pool(self.pool_unconf))
