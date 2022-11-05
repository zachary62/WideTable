import unittest

import duckdb

from joinboost.cjt import CJT
from joinboost.executor import ExecutorFactory
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
        join_graph.add_relation('R', relation_address='../data/synthetic-many-to-many/R.csv')
        join_graph.add_relation('S', relation_address='../data/synthetic-many-to-many/S.csv')
        join_graph.add_relation('T', relation_address='../data/synthetic-many-to-many/T.csv')
        join_graph.add_join('R', 'S', ['B'], ['B']);
        join_graph.add_join('S', 'T', ['B'], ['B']);
        return CJT(semi_ring=varSemiRing(), join_graph=join_graph)
        
    """
    Join Graph for data/synthetic-one_to_many/
    R(ABDH) - T(BFK) - S(FE)
    """
    def initialize_synthetic_one_to_many(self):
        duck_db_conn = duckdb.connect(database=':memory:')
        join_graph = JoinGraph(duck_db_conn)
        join_graph.add_relation('R', relation_address='../data/synthetic-one-to-many/R.csv')
        join_graph.add_relation('G', relation_address='../data/synthetic-one-to-many/S.csv')
        join_graph.add_relation('T', relation_address='../data/synthetic-one-to-many/T.csv')
        join_graph.add_join('R', 'T', ['B'], ['B']);
        join_graph.add_join('G', 'T', ['F'], ['F']);
        return CJT(semi_ring=varSemiRing(), join_graph=join_graph)
    
    """
    Tests if message passing works in many to many join graph.
    Following tables have many to many cardinality on B.
    Test Query:
        SELECT SUM(A), count(*), R.B
        FROM R join G on R.B = G.B join T on R.B = T.B
        GROUP BY R.B
    """
    def test_many_to_many(self):
        cjt = self.initialize_synthetic_many_to_many()
        cjt.set_root_relation('T')
        cjt.lift(relation='R', attr='A')
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], mode=3)
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(A), count(*), R.B 
            FROM R join S on R.B = S.B join T on R.B = T.B 
            GROUP BY R.B
            """).fetchall()
        self.assertEqual(actual, expected)

    """
    Tests if message passing works in one to many join graph. 
    Similar to star schema with Fact Table. Here, T is the fact table
    Test Query:
        SELECT SUM(K), count(*), R.B 
        FROM R join T on R.B = T.B join G on G.F = T.F 
        GROUP BY R.B
    """
    def test_one_to_many(self):
        cjt = self.initialize_synthetic_one_to_many()
        cjt.set_root_relation('T')
        cjt.lift(relation='T', attr='K')
        cjt.calibration()
        actual = cjt.absorption('joinboost_tmp_0', ['B'], mode=3)
        expected = cjt.exe.conn.execute(
            """
            SELECT SUM(T.K), count(*), T.B 
            FROM R join T on R.B = T.B join G on G.F = T.F 
            GROUP BY T.B
            """
            ).fetchall()
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
