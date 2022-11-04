import unittest

import duckdb

from src.joinboost.cjt import CJT
from src.joinboost.executor import ExecutorFactory
from src.joinboost.joingraph import JoinGraph
from src.joinboost.semiring import varSemiRing


class TestCJT(unittest.TestCase):

    """
    Tests if message passing works in many to many join graph.
    Following tables have many to many cardinality on B.
    Test Query:
        SELECT SUM(A), count(*), R.B
        FROM R join G on R.B = G.B join T on R.B = T.B
        GROUP BY R.B
    """
    def test_many_to_many(self):
        duck_db_conn = duckdb.connect(database=':memory:')
        con = ExecutorFactory(con=duck_db_conn)

        join_graph = JoinGraph(exe=con)
        join_graph.add_relation('R', relation_address='../data/synthetic-many-to-many/R.csv')
        join_graph.add_relation('G', relation_address='../data/synthetic-many-to-many/S.csv')
        join_graph.add_relation('T', relation_address='../data/synthetic-many-to-many/T.csv')
        join_graph.add_join('R', 'G', ['B'], ['B']);
        join_graph.add_join('G', 'T', ['B'], ['B']);
        cjt = CJT(semi_ring=varSemiRing(), join_graph=join_graph)
        cjt.set_root_relation('T')
        cjt.lift(relation='R', attr='A')
        cjt.calibration()
        actual = cjt.absorption('T', ['B'], mode=3)
        expected = con.conn.execute(
            "SELECT SUM(A), count(*), R.B FROM R join G on R.B = G.B join T on R.B = T.B GROUP BY R.B").fetchall()
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
        duck_db_conn = duckdb.connect(database=':memory:')
        con = ExecutorFactory(con=duck_db_conn)

        join_graph = JoinGraph(exe=con)
        join_graph.add_relation('R', relation_address='../data/synthetic-one-to-many/R.csv')
        join_graph.add_relation('G', relation_address='../data/synthetic-one-to-many/S.csv')
        join_graph.add_relation('T', relation_address='../data/synthetic-one-to-many/T.csv')
        join_graph.add_join('R', 'T', ['B'], ['B']);
        join_graph.add_join('G', 'T', ['F'], ['F']);
        cjt = CJT(semi_ring=varSemiRing(), join_graph=join_graph)
        cjt.set_root_relation('T')
        cjt.lift(relation='T', attr='K')
        cjt.calibration()
        actual = cjt.absorption('joinboost_tmp_0', ['B'], mode=3)
        expected = con.conn.execute(
            "SELECT SUM(T.K), count(*), T.B FROM R join T on R.B = T.B join G on G.F = T.F GROUP BY T.B").fetchall()
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
