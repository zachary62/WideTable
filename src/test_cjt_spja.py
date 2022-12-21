import sys
import unittest

import duckdb

from widetable.semiring import AvgSemiRing, CountSemiRing, SumSemiRing
from widetable.aggregator import Annotation
from test_utils import initialize_synthetic_one_to_many, initialize_synthetic_many_to_many, \
    initialize_tpch_for_sample_query, initialize_tpch_small


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


    def test_tpch_small_groupby_selection(self):
        cjt = initialize_tpch_small()
        expected = cjt.exe.conn.execute(
        """
        SELECT SUM(o.o_totalprice), c.c_name as c_name
        FROM customer c join orders o on c.c_custkey = o.o_custkey
        WHERE c.c_name = 'Customer#000000001'
        GROUP BY c.c_name
        ORDER BY c.c_name
        """).fetchall()
        cjt.lift_all()
        cjt.add_groupbys('customer', 'c_name')
        cjt.add_annotations('customer', ['c_name', Annotation.NOT_DISTINCT, 'Customer#000000001'])
        cjt.upward_message_passing(cjt.get_relation_from_user_table('customer'))
        actual = cjt.absorption('customer', ['c_name'], ['c_name'], mode=3)
        self.assertEqual(expected, actual)


    def test_tpch_small_sample_query(self):
        cjt = initialize_tpch_for_sample_query()
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(o_totalprice), n_name
            FROM customer, orders, nation, region
            WHERE c_custkey = o_custkey
             AND c_nationkey = n_nationkey
             AND n_regionkey = r_regionkey
             AND r_name = 'ASIA'
            GROUP BY n_name
            ORDER BY n_name
            """).fetchall()
        cjt.lift_all()
        cjt.add_groupbys('nation', 'n_name')
        cjt.add_annotations('region', ['r_name', Annotation.NOT_DISTINCT, 'ASIA'])
        cjt.upward_message_passing(cjt.get_relation_from_user_table('nation'))
        actual = cjt.absorption('nation', ['n_name'], ['n_name'], mode=3)
        for i in range(len(expected)):
            self.assertTrue(abs(expected[i][0]-actual[i][0])<1e-5)
            self.assertEqual(expected[i][1], actual[i][1])


if __name__ == '__main__':
    unittest.main()
