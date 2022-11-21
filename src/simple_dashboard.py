import duckdb

from joinboost.cjt import CJT
from joinboost.joingraph import JoinGraph
from joinboost.semiring import AvgSemiRing
from joinboost.aggregator import Annotation
from joinboost.dashboard import DashBoard

semi_ring=AvgSemiRing(relation='R', attr='A')
duck_db_conn = duckdb.connect(database=':memory:')
join_graph = JoinGraph(duck_db_conn)
join_graph.add_relation('R', attrs=["A","D","H"], 
                 relation_address='../data/synthetic-many-to-many/R.csv')
join_graph.add_relation('S', attrs=["E"], 
                 relation_address='../data/synthetic-many-to-many/S.csv')
join_graph.add_relation('T', attrs=["F"], 
                 relation_address='../data/synthetic-many-to-many/T.csv')
join_graph.add_join('R', 'S', ['B'], ['B']);
join_graph.add_join('S', 'T', ['B'], ['B']);

dashboard = DashBoard(join_graph)
dashboard.register_measurement("avg",'R','A')