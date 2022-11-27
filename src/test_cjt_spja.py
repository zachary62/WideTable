import unittest

import duckdb

from widetable.cjt import CJT
from widetable.joingraph import JoinGraph
from widetable.semiring import AvgSemiRing, CountSemiRing, SumSemiRing
from widetable.aggregator import Annotation
from test_utils import initialize_synthetic_one_to_many, initialize_synthetic_many_to_many


class TestCJT(unittest.TestCase):

    """
    Tests if message passing works in one to many join graph with selection.
    Similar to star schema with Fact Table. Here, T is the fact table
    Test Query:
        SELECT SUM(K), count(*), R.B 
        FROM R join T on R.B = T.B join S on S.F = T.F 
        WHERE S.E = 2
        GROUP BY T.B ORDER BY T.B
    """

    def test_one_to_many_with_selection(self):
        cjt = initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(user_table='T', attr='K'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B 
            FROM R join T on R.B = T.B join S on S.F = T.F 
            WHERE S.E = 2
            GROUP BY T.B ORDER BY T.B
            """
        ).fetchall()
        cjt.lift_all()
        cjt.calibration()
        cjt.add_annotations('S', ['E', Annotation.NOT_DISTINCT, '2'])
        cjt.calibration()
        # redundant calibration should not break 
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)
        
    def test_many_to_many_with_selection(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(user_table='R', attr='A'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            WHERE S.B = 2
            GROUP BY R.B ORDER BY R.B
            """).fetchall()
        
        cjt.lift_all()
        cjt.calibration()
        cjt.add_annotations('S', ['B', Annotation.NOT_DISTINCT, '2'])
        cjt.calibration()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)

    """
    Tests if message passing works in join graph with groupby.
    """
    def test_one_to_many_with_groupby(self):
        cjt = initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(user_table='T', attr='K'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B, S.E
            FROM R join T on R.B = T.B join S on S.F = T.F 
            GROUP BY T.B, S.E ORDER BY T.B, E
            """
        ).fetchall()
        cjt.lift_all()
        cjt.calibration()
        cjt.add_groupbys('S', 'E')
        cjt.calibration()
        # redundant calibration should not break 
        cjt.calibration()
        actual = cjt.absorption('T', ['B', 'E'], ['B', 'E'], mode=3)
        self.assertEqual(expected, actual)

    def test_many_to_many_with_groupby(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(user_table='T', attr='F'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.F), count(*), T.B, S.E
            FROM R join T on R.B = T.B join S on S.B = T.B 
            GROUP BY T.B, S.E ORDER BY T.B, E
            """
        ).fetchall()
        cjt.lift_all()
        cjt.calibration()
        cjt.add_groupbys('S', 'E')
        # upward message passing is using internal table name 
        # because it is supposed to be an internal function
        cjt.upward_message_passing(cjt.get_relation_from_user_table('T'))
        actual = cjt.absorption('T', ['B', 'E'], ['B', 'E'], mode=3)
        self.assertEqual(expected, actual)
        
if __name__ == '__main__':
    unittest.main()
