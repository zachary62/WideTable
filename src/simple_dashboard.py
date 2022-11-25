import duckdb

from joinboost.cjt import CJT
from joinboost.joingraph import JoinGraph
from joinboost.semiring import AvgSemiRing
from joinboost.aggregator import Annotation
from joinboost.dashboard import DashBoard
from joinboost.scope import *

# semi_ring=AvgSemiRing(relation='R', attr='A')
# duck_db_conn = duckdb.connect(database=':memory:')
# join_graph = JoinGraph(duck_db_conn)
# join_graph.add_relation('R', attrs=["A","D","H"], 
#                  relation_address='../data/synthetic-many-to-many/R.csv')
# join_graph.add_relation('S', attrs=["E"], 
#                  relation_address='../data/synthetic-many-to-many/S.csv')
# join_graph.add_relation('T', attrs=["F"], 
#                  relation_address='../data/synthetic-many-to-many/T.csv')
# join_graph.add_join('R', 'S', ['B'], ['B']);
# join_graph.add_join('S', 'T', ['B'], ['B']);

# dashboard = DashBoard(join_graph)
# dashboard.register_measurement("avg",'R','A')


duck_db_conn = duckdb.connect(database=':memory:')
join_graph = JoinGraph(duck_db_conn)
dashboard = DashBoard(join_graph)
dashboard.add_relation('orders', relation_address='../data/tpch_10mb/orders.parquet')
dashboard.add_relation('lineitem', relation_address='../data/tpch_10mb/lineitem.parquet')
dashboard.add_relation('partsupp', relation_address='../data/tpch_10mb/partsupp.parquet')
dashboard.add_relation('part', relation_address='../data/tpch_10mb/part.parquet')

dashboard.add_join('lineitem', 'orders', ['l_orderkey'], ['o_orderkey']);
dashboard.add_join('lineitem', 'partsupp', ['l_suppkey','l_partkey'], ['ps_suppkey','ps_partkey']);
dashboard.add_join('partsupp', 'part', ['ps_partkey'], ['p_partkey']);

measurement1 = dashboard.register_measurement("sum",'part','p_retailprice', scope=FullJoin())
measurement2 = dashboard.register_measurement("sum",'partsupp','ps_availqty', scope=SingleRelation('partsupp'))

# this shows the visualization of dashboard
dashboard

dashboard.absorption(measurement1, mode=5)
dashboard.absorption(measurement2, mode=5)