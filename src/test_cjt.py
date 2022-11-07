import unittest

import duckdb

from joinboost.cjt import CJT
from joinboost.joingraph import JoinGraph
from joinboost.semiring import varSemiRing


class TestCJT(unittest.TestCase):
    """
    Join Graph for data/synthetic-many-to-many/
    T(BF) - R(ABDH) - S(BE)
    """
    def initialize_synthetic_many_to_many(self):
        duck_db_conn = duckdb.connect(database=':memory:')
        join_graph = JoinGraph(duck_db_conn)
        cjt = CJT(semi_ring=varSemiRing(), join_graph=join_graph)
        cjt.add_relation('R', relation_address='../data/synthetic-many-to-many/R.csv')
        cjt.add_relation('S', relation_address='../data/synthetic-many-to-many/S.csv')
        cjt.add_relation('T', relation_address='../data/synthetic-many-to-many/T.csv')
        cjt.add_join('R', 'S', ['B'], ['B']);
        cjt.add_join('S', 'T', ['B'], ['B']);
        return cjt
        
    """
    Join Graph for data/synthetic-one_to_many/
    R(ABDH) - T(BFK) - S(FE)
    """
    def initialize_synthetic_one_to_many(self):
        duck_db_conn = duckdb.connect(database=':memory:')
        join_graph = JoinGraph(duck_db_conn)
        cjt = CJT(semi_ring=varSemiRing(), join_graph=join_graph)
        cjt.add_relation('R', relation_address='../data/synthetic-one-to-many/R.csv')
        cjt.add_relation('S', relation_address='../data/synthetic-one-to-many/S.csv')
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
        cjt = self.initialize_synthetic_many_to_many()
        cjt.lift(relation='R', attr='A')
        cjt.calibration('T')
        actual = cjt.absorption('T', ['B'], mode=3)
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            GROUP BY R.B ORDER BY R.B
            """).fetchall()
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
        cjt = self.initialize_synthetic_one_to_many()
        cjt.lift(relation='T', attr='K')
        cjt.calibration('joinboost_tmp_0')
        actual = cjt.absorption('joinboost_tmp_0', ['B'], mode=3)
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B 
            FROM R join T on R.B = T.B join S on S.F = T.F 
            GROUP BY T.B ORDER BY T.B
            """
            ).fetchall()
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
