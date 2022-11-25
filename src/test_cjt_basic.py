import unittest

import duckdb

from joinboost.cjt import CJT
from joinboost.joingraph import JoinGraph
from joinboost.semiring import AvgSemiRing, CountSemiRing, SumSemiRing
from joinboost.aggregator import Annotation
from test_utils import initialize_synthetic_one_to_many, initialize_synthetic_many_to_many


class TestCJT(unittest.TestCase):

    """
    Tests if message passing works in many to many join graph.
    Following tables have many to many multiplicity on B.
    Test Query:
        SELECT SUM(A), count(*), R.B
        FROM R join G on R.B = G.B join T on R.B = T.B
        GROUP BY R.B ORDER BY R.B
    """

    def test_many_to_many(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(user_table='R', attr='A'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            GROUP BY R.B ORDER BY R.B
            """).fetchall()
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)

    """
    Tests if message passing works in one to many join graph. 
    Similar to star schema with Fact Table. Here, T is the fact table
    Test Query:
        SELECT SUM(K), count(*), R.B 
        FROM R join T on R.B = T.B join S on S.F = T.F 
        GROUP BY T.B ORDER BY T.B
    """

    def test_one_to_many(self):
        cjt = initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(user_table='T', attr='K'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B 
            FROM R join T on R.B = T.B join S on S.F = T.F 
            GROUP BY T.B ORDER BY T.B
            """
        ).fetchall()
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)
        
    """
    Tests upward message passing without calibration
    """
    def test_many_to_many_upward(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(user_table='R', attr='A'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            GROUP BY R.B ORDER BY R.B
            """).fetchall()
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)
        
    def test_many_to_many_for_count_semiring(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=CountSemiRing(user_table='T'))
        expected = cjt.exe.conn.execute(
            """
            SELECT count(*), T.B
            FROM R join T on R.B = T.B join S on S.B = T.B 
            GROUP BY T.B ORDER BY T.B
            """
        ).fetchall()
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)

    def test_many_to_many_for_sum_semiring(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=SumSemiRing(user_table='T', attr='F'))
        expected = cjt.exe.conn.execute(
            """
            SELECT sum(T.F), T.B
            FROM R join T on R.B = T.B join S on S.B = T.B 
            GROUP BY T.B ORDER BY T.B
            """
        ).fetchall()
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()
