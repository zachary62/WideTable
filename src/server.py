from flask import Flask, render_template, jsonify, request

import duckdb
from widetable.cjt import CJT
from widetable.joingraph import JoinGraph
from widetable.semiring import AvgSemiRing
from widetable.aggregator import Annotation
from widetable.dashboard import DashBoard
from widetable.scope import *


app = Flask(__name__)

duck_db_conn = duckdb.connect(database=':memory:')
join_graph = JoinGraph(duck_db_conn)
dashboard = DashBoard(join_graph)
dashboard.add_relation('orders', relation_address='../data/tpch_10mb/orders.parquet')
dashboard.add_relation('lineitem', relation_address='../data/tpch_10mb/lineitem.parquet')
dashboard.add_relation('partsupp', relation_address='../data/tpch_10mb/partsupp.parquet')
dashboard.add_relation('part', relation_address='../data/tpch_10mb/part.parquet')
dashboard.add_relation('nation', relation_address='../data/tpch_10mb/nation.parquet')
dashboard.add_relation('supplier', relation_address='../data/tpch_10mb/supplier.parquet')
dashboard.add_relation('region', relation_address='../data/tpch_10mb/region.parquet')
dashboard.add_relation('customer', relation_address='../data/tpch_10mb/customer.parquet')

dashboard.add_join('lineitem', 'orders', ['l_orderkey'], ['o_orderkey'])
dashboard.add_join('orders', 'customer', ['o_custkey'], ['c_custkey'])
dashboard.add_join('partsupp', 'supplier', ['ps_suppkey'], ['s_suppkey'])
dashboard.add_join('customer', 'nation', ['c_nationkey'], ['n_nationkey'])
dashboard.add_join('nation', 'region', ['n_regionkey'], ['r_regionkey'])
dashboard.add_join('lineitem', 'partsupp', ['l_suppkey','l_partkey'], ['ps_suppkey','ps_partkey'])
dashboard.add_join('partsupp', 'part', ['ps_partkey'], ['p_partkey'])
dashboard.register_measurement("sum",'part','p_retailprice', scope=ReplicateFact('part', 'part'))
dashboard.register_measurement("sum",'lineitem','l_extendedprice * (1 - l_discount)', scope=ReplicateFact('lineitem', 'lineitem'))

@app.route('/')
def homepage():
    return render_template('index.html')


@app.route('/get_relation_sample', methods=['POST'])
def get_relation_sample():
    print(request)
    # Get the string from the request data
    data = request.get_json()
    
    relation = data["relation"]
    agg_exprs = data.get("agg_exprs", None)
    # converting between text to python enum
    if agg_exprs is not None:
        for k, v in agg_exprs.items():
            agg_exprs[k] = (v[0], Aggregator[v[1]])
    selection_conds = data["selection_conds"]
    groupby_conds = data.get("groupby_conds") or []
    orderby_conds = data.get("orderby_conds") or []
    limit = data.get("limit", 100)
    # Return the sample data
    return jsonify(dashboard.get_relation_sample(relation, selection_conds, groupby_conds, orderby_conds, agg_exprs, limit))

@app.route('/get_graph')
def get_graph():
    nodes, links = dashboard.get_graph()
    return jsonify({'nodes': nodes, 'links': links})