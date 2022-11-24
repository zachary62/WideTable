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
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(relation='R', attr='A'))
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
        cjt = initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(relation='T', attr='K'))
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
    Tests if message passing works in one to many join graph with selection.
    Similar to star schema with Fact Table. Here, T is the fact table
    Test Query:
        SELECT SUM(K), count(*), R.B 
        FROM R join T on R.B = T.B join S on S.F = T.F 
        WHERE S.E = 2
        GROUP BY T.B ORDER BY T.B
    """

    def test_one_to_many_with_selection(self):
        cjt = initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(relation='T', attr='K'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B 
            FROM R join T on R.B = T.B join S on S.F = T.F 
            WHERE S.E = 2
            GROUP BY T.B ORDER BY T.B
            """
        ).fetchall()
        cjt.add_annotations('S', ['E', Annotation.NOT_DISTINCT, '2'])
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)

    """
    Tests if message passing works in many to many join graph.
    Following tables have many to many multiplicity on B.
    Test Query:
        SELECT SUM(A), count(*), R.B
        FROM R join G on R.B = G.B join T on R.B = T.B
        WHERE G.B = 2
        GROUP BY R.B ORDER BY R.B
    """

    def test_many_to_many_with_selection(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(relation='R', attr='A'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            WHERE S.B = 2
            GROUP BY R.B ORDER BY R.B
            """).fetchall()

        cjt.add_annotations('S', ['B', Annotation.NOT_DISTINCT, '2'])
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(expected, actual)

    # TODO: add more tests for group-by
    def test_one_to_many_with_groupby(self):
        cjt = initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(relation='T', attr='K'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B, S.E
            FROM R join T on R.B = T.B join S on S.F = T.F 
            GROUP BY T.B, S.E ORDER BY T.B, E
            """
        ).fetchall()
        cjt.add_groupbys('S', 'E')
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B', 'E'], ['B', 'E'], mode=3)
        self.assertEqual(expected, actual)

    def test_many_to_many_with_groupby(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(relation='T', attr='F'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.F), count(*), T.B, S.E
            FROM R join T on R.B = T.B join S on S.B = T.B 
            GROUP BY T.B, S.E ORDER BY T.B, E
            """
        ).fetchall()
        cjt.add_groupbys('S', 'E')
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B', 'E'], ['B', 'E'], mode=3)
        self.assertEqual(expected, actual)

    def test_many_to_many_for_count_semiring(self):
        cjt = initialize_synthetic_many_to_many(semi_ring=CountSemiRing(relation='T'))
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
        cjt = initialize_synthetic_many_to_many(semi_ring=SumSemiRing(relation='T', attr='F'))
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
