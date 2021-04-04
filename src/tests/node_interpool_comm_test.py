import unittest
import string
import random
import node_interpool_comm as nic
import uuid

def res_list_to_str(list_):
    result = ""
    for a in list_:
        if type(a) == list or type(a) == tuple:
            result += res_list_to_str(a)
        else:
            result += str(a)
    return result


def random_string(n):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(n))


class TestNodeInterpoolComm(unittest.TestCase):
    def setUp(self):
        self.msg = "dfsdouigfsdiufgsdf"
        self.targets = [uuid.uuid4() for _ in range(5)]
        self.src = [uuid.uuid4() for _ in range(5)]
        
        self.input_sets = []
        for i in range(len(self.src)):
            me = self.src[i]
            others = self.src[:i]+self.src[i+1:]
            self.input_sets.append((me, others))

    def test_hash_equality(self):
        res_str_pre = ""

        for i in range(5):
            me = self.input_sets[i][0]
            others = self.input_sets[i][1]

            src_pool, trgt_pool, hsh_fr_trgt = nic.get_lists_for_nodes(me, others, self.targets)

            array = nic.get_closest_node(self.msg.encode(), trgt_pool, hsh_fr_trgt)

            results = nic.pick_two_equal_distrib(array, trgt_pool, src_pool)

            print(results)

            res_str = res_list_to_str(results.values())

            if(i>0):
                self.assertEqual(res_str, res_str_pre, "Hashes of same sets are not equal")

            res_str_pre = res_str

    def test_equal_source_distribution(self):
        src_pool, trgt_pool, hsh_fr_trgt = nic.get_lists_for_nodes(self.src[0], self.src[1:], self.targets)
        result_cont = {t: 0 for t in src_pool}

        for a in range(5000):
            str_ = random_string(random.randint(10,100))
            res_list = nic.calculate_allocs(str_.encode(), trgt_pool, src_pool, hsh_fr_trgt)
            
            resorted = nic.get_list_by_sender(res_list)

            for l in res_list.values():
                for p in l:
                    result_cont[p[0]] += 1

        default = 0
        for index, value in enumerate(result_cont.values()):

            if index > 0:
                self.assertEqual(default, value, "Number of targets are not equal for all sources")
            
            default = value
