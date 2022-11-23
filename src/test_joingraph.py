import unittest
import pandas as pd
import test_utils
from joinboost.joingraph import JoinGraph, JoinGraphException


class TestJoingraph(unittest.TestCase):

    def test_cycle(self):
        R = pd.DataFrame(columns=['A', 'B'])
        S = pd.DataFrame(columns=['B', 'C'])
        T = pd.DataFrame(columns=['A', 'C'])

        dataset = JoinGraph()
        try:
            dataset.add_relation('R', ['B', 'E'], relation_address=R)
            raise JoinGraphException('Attribute not in the relation but is allowed!')
        except:
            pass
        dataset.add_relation('R', ['B', 'A'], relation_address=R)
        dataset.add_relation('S', ['B', 'C'], relation_address=S)
        dataset.add_relation('T', ['A', 'C'], relation_address=T)

        try:
            dataset._preprocess()
            raise Exception('Disjoint join graph is allowed!')
        except JoinGraphException:
            pass

        dataset.add_join("R", "S", ["B"], ["B"])
        dataset.add_join("S", "T", ["C"], ["C"])
        dataset._preprocess()

        # TODO: check join keys available
        # dataset.add_join("R", "S", ["A"], ["A"])

        dataset.add_join("R", "T", ["A"], ["A"])
        try:
            dataset._preprocess()
            raise Exception('Cyclic join graph is allowed!')
        except JoinGraphException:
            pass

    def test_multiplicity_for_many_to_many(self):
        cjt = test_utils.initialize_synthetic_many_to_many()

        self.assertGreater(cjt.multiplicity['R']['S'], 1)
        self.assertGreater(cjt.multiplicity['S']['R'], 1)
        self.assertGreater(cjt.multiplicity['S']['T'], 1)
        self.assertGreater(cjt.multiplicity['T']['S'], 1)

        self.assertEqual(cjt.missing_keys['S']['R'], 1)
        self.assertNotIn('S', cjt.missing_keys['R'])
        self.assertNotIn('R', cjt.missing_keys['T'])
        self.assertNotIn('T', cjt.missing_keys['R'])

    def test_multiplicity_for_one_to_many(self):
        cjt = test_utils.initialize_synthetic_one_to_many()

        self.assertGreater(cjt.multiplicity['R']['T'], 1)
        self.assertEqual(cjt.multiplicity['T']['R'], 1)
        self.assertGreater(cjt.multiplicity['S']['T'], 1)
        self.assertEqual(cjt.multiplicity['T']['S'], 1)

        self.assertEqual(cjt.missing_keys['S']['T'], 1)
        self.assertNotIn('S', cjt.missing_keys['T'])
        self.assertNotIn('R', cjt.missing_keys['T'])
        self.assertNotIn('T', cjt.missing_keys['R'])


if __name__ == '__main__':
    unittest.main()
