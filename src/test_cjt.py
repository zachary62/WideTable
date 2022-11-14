import unittest

import duckdb

from joinboost.cjt import CJT
from joinboost.joingraph import JoinGraph
from joinboost.semiring import AvgSemiRing
from joinboost.aggregator import Annotation


class TestCJT(unittest.TestCase):
    """
    Join Graph for data/synthetic-many-to-many/
    S(BE) - T(BF) - R(ABDH) 
    """
    def initialize_synthetic_many_to_many(self, semi_ring=AvgSemiRing()):
        duck_db_conn = duckdb.connect(database=':memory:')
        join_graph = JoinGraph(duck_db_conn)
        cjt = CJT(semi_ring=semi_ring, join_graph=join_graph)
        cjt.add_relation('R', attrs=["D","H"], 
                         relation_address='../data/synthetic-many-to-many/R.csv')
        cjt.add_relation('S', attrs=["E"], 
                         relation_address='../data/synthetic-many-to-many/S.csv')
        cjt.add_relation('T', attrs=["F"], 
                         relation_address='../data/synthetic-many-to-many/T.csv')
        cjt.add_join('R', 'S', ['B'], ['B']);
        cjt.add_join('S', 'T', ['B'], ['B']);
        return cjt

    """
    Join Graph for data/synthetic-one_to_many/
    R(ABDH) - T(BFK) - S(FE)
    """
    def initialize_synthetic_one_to_many(self, semi_ring=AvgSemiRing()):
        duck_db_conn = duckdb.connect(database=':memory:')
        join_graph = JoinGraph(duck_db_conn)
        cjt = CJT(semi_ring=semi_ring, join_graph=join_graph)
        cjt.add_relation('R', attrs=["A", "D", "H"], relation_address='../data/synthetic-one-to-many/R.csv')
        cjt.add_relation('S', attrs=["E"], relation_address='../data/synthetic-one-to-many/S.csv')
        cjt.add_relation('T', relation_address='../data/synthetic-one-to-many/T.csv')
        cjt.add_join('R', 'T', ['B'], ['B']);
        cjt.add_join('S', 'T', ['F'], ['F']);
        return cjt

    """
    Tests if message passing works in many to many join graph.
    Following tables have many to many cardinality on B.
    Test Query:
        SELECT SUM(A), count(*), R.B
        FROM R join G on R.B = G.B join T on R.B = T.B
        GROUP BY R.B ORDER BY R.B
    """
    def test_many_to_many(self):
        cjt = self.initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(relation='R', attr='A'))
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            GROUP BY R.B ORDER BY R.B
            """).fetchall()
        cjt.lift_all()
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], ['B'], mode=3)
        self.assertEqual(actual, expected)

    """
    Tests if message passing works in one to many join graph. 
    Similar to star schema with Fact Table. Here, T is the fact table
    Test Query:
        SELECT SUM(K), count(*), R.B 
        FROM R join T on R.B = T.B join S on S.F = T.F 
        GROUP BY T.B ORDER BY T.B
    """
    def test_one_to_many(self):
        cjt = self.initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(relation='T', attr='K'))
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
        self.assertEqual(actual, expected)

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
        cjt = self.initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(relation='T', attr='K'))
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
        self.assertEqual(actual, expected)

    """
    Tests if message passing works in many to many join graph.
    Following tables have many to many cardinality on B.
    Test Query:
        SELECT SUM(A), count(*), R.B
        FROM R join G on R.B = G.B join T on R.B = T.B
        WHERE G.B = 2
        GROUP BY R.B ORDER BY R.B
    """
    def test_many_to_many_with_selection(self):
        cjt = self.initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(relation='R', attr='A'))
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
        self.assertEqual(actual, expected)
        
    # TODO: add more tests for group-by
    def test_one_to_many_with_groupby(self):
        cjt = self.initialize_synthetic_one_to_many(semi_ring=AvgSemiRing(relation='T', attr='K'))
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
        actual = cjt.absorption('T', ['B','E'], ['B','E'], mode=3)
        self.assertEqual(actual, expected)

    def test_many_to_many_with_groupby(self):
        cjt = self.initialize_synthetic_many_to_many(semi_ring=AvgSemiRing(relation='T', attr='F'))
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
        actual = cjt.absorption('T', ['B','E'], ['B','E'], mode=3)
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
