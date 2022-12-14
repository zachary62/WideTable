import duckdb

from widetable.cjt import CJT
from widetable.joingraph import JoinGraph
from widetable.semiring import AvgSemiRing, SumSemiRing
from widetable.dashboard import DashBoard


def initialize_synthetic_single(semi_ring=AvgSemiRing()):
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    cjt = CJT(semi_ring=semi_ring, join_graph=join_graph)
    cjt.add_relation('R', attrs=["A", "D", "H"], relation_address='../data/synthetic-one-to-many/R.csv')
    return cjt


"""
Join Graph for data/synthetic-one_to_many/
R(ABDH) - T(BFK) - S(FE)
"""
def initialize_synthetic_one_to_many(semi_ring=AvgSemiRing()):
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    cjt = CJT(semi_ring=semi_ring, join_graph=join_graph)
    cjt.add_relation('R', attrs=["A", "D", "H"], relation_address='../data/synthetic-one-to-many/R.csv')
    cjt.add_relation('S', attrs=["E"], relation_address='../data/synthetic-one-to-many/S.csv')
    cjt.add_relation('T', relation_address='../data/synthetic-one-to-many/T.csv')
    cjt.add_join('R', 'T', ['B'], ['B'])
    cjt.add_join('S', 'T', ['F'], ['F'])
    return cjt

    """
    Join Graph for data/synthetic-many-to-many/
    S(BE) - T(BF) - R(ABDH) 
    """
    

def initialize_synthetic_many_to_many(semi_ring=AvgSemiRing()):
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    cjt = CJT(semi_ring=semi_ring, join_graph=join_graph)
    cjt.add_relation('R', attrs=["D", "H"],
                     relation_address='../data/synthetic-many-to-many/R.csv')
    cjt.add_relation('S', attrs=["E"],
                     relation_address='../data/synthetic-many-to-many/S.csv')
    cjt.add_relation('T', attrs=["F"],
                     relation_address='../data/synthetic-many-to-many/T.csv')
    cjt.add_join('R', 'S', ['B'], ['B'])
    cjt.add_join('S', 'T', ['B'], ['B'])
    return cjt


def initialize_tpch_small_dashboard():
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
    return dashboard

def initialize_tpch_for_sample_query():
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    cjt = CJT(semi_ring=SumSemiRing('orders', 'o_totalprice'), join_graph=join_graph)
    cjt.add_relation('orders', relation_address='../data/tpch_10mb/orders.parquet')
    cjt.add_relation('customer', relation_address='../data/tpch_10mb/customer.parquet')
    cjt.add_relation('nation', relation_address='../data/tpch_10mb/nation.parquet')
    cjt.add_relation('region', relation_address='../data/tpch_10mb/region.parquet')

    cjt.add_join('customer', 'orders', ['c_custkey'], ['o_custkey']);
    cjt.add_join('region', 'nation', ['r_regionkey'], ['n_regionkey']);
    cjt.add_join('nation', 'customer', ['n_nationkey'], ['c_nationkey']);
    return cjt


def initialize_tpch_small():
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    cjt = CJT(semi_ring=SumSemiRing('orders','o_totalprice'), join_graph=join_graph)
    cjt.add_relation('orders', relation_address='../data/tpch_10mb/orders.parquet')
    cjt.add_relation('customer', relation_address='../data/tpch_10mb/customer.parquet')

    cjt.add_join('customer', 'orders', ['c_custkey'], ['o_custkey']);
    return cjt

