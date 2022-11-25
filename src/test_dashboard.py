import unittest

import duckdb

from joinboost.cjt import CJT
from joinboost.joingraph import JoinGraph
from joinboost.semiring import AvgSemiRing, CountSemiRing, SumSemiRing
from joinboost.aggregator import Annotation
from test_utils import initialize_tpch_small_dashboard
from joinboost.scope import *


class TestDashboard(unittest.TestCase):

    def test_full_join(self):
        dashboard = initialize_tpch_small_dashboard()
        expected = dashboard.exe.conn.execute(
            """
            SELECT SUM(p_retailprice)
            FROM lineitem join orders on l_orderkey = o_orderkey
                 join partsupp on (l_suppkey=ps_suppkey and l_partkey=ps_partkey) 
                 join part on ps_partkey = p_partkey
            """
        ).fetchall()
        measurement = dashboard.register_measurement("sum",'part','p_retailprice', scope=FullJoin())
        actual = dashboard.absorption(measurement, mode=3)
        self.assertTrue(abs(expected[0][0] - actual[0][0])< 1e-5)
    
    def test_single_relation(self):
        dashboard = initialize_tpch_small_dashboard()
        expected = dashboard.exe.conn.execute(
            """
            SELECT SUM(ps_availqty)
            FROM partsupp
            """
        ).fetchall()
        measurement = dashboard.register_measurement("sum",'partsupp','ps_availqty', scope=SingleRelation('partsupp'))
        actual = dashboard.absorption(measurement, mode=3)
        self.assertTrue(abs(expected[0][0] - actual[0][0])< 1e-5)
        
    def test_replicate_fact_1(self):
        dashboard = initialize_tpch_small_dashboard()
        scope = ReplicateFact("part","partsupp")
        measurement = dashboard.register_measurement("sum",'part','p_retailprice',scope=scope)
        self.assertTrue(("part", "partsupp") in scope.edges or ("partsupp", "part") in scope.edges)
        self.assertFalse(("lineitem", "partsupp") in scope.edges or ("partsupp", "lineitem") in scope.edges)
        
    def test_replicate_fact_2(self):
        dashboard = initialize_tpch_small_dashboard()
        scope = ReplicateFact("part","lineitem")
        measurement = dashboard.register_measurement("sum",'part','p_retailprice',scope=scope)
        self.assertTrue(("part", "partsupp") in scope.edges or ("partsupp", "part") in scope.edges)
        self.assertTrue(("lineitem", "partsupp") in scope.edges or ("partsupp", "lineitem") in scope.edges)
        
    def test_replicate_fact_2(self):
        dashboard = initialize_tpch_small_dashboard()
        scope = ReplicateFact("partsupp","partsupp")
        measurement = dashboard.register_measurement("sum",'partsupp','ps_availqty',scope=scope)
        self.assertTrue(("part", "partsupp") in scope.edges or ("partsupp", "part") in scope.edges)
        self.assertFalse(("lineitem", "partsupp") in scope.edges or ("partsupp", "lineitem") in scope.edges)
    
if __name__ == '__main__':
    unittest.main()
