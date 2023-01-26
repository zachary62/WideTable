import json
import math
from datetime import datetime

from flask import Flask, render_template, jsonify, request

import duckdb
from widetable.cjt import CJT
from widetable.joingraph import JoinGraph
from widetable.semiring import AvgSemiRing
from widetable.aggregator import Annotation
from widetable.dashboard import DashBoard
from widetable.scope import *

app = Flask(__name__)

def generate_tpch_dashboard():
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    dashboard = DashBoard(join_graph)
    dashboard.add_relation(
        'orders', relation_address='../data/tpch_10mb/orders.parquet')
    dashboard.add_relation(
        'lineitem', relation_address='../data/tpch_10mb/lineitem.parquet')
    dashboard.add_relation(
        'partsupp', relation_address='../data/tpch_10mb/partsupp.parquet')
    dashboard.add_relation(
        'part', relation_address='../data/tpch_10mb/part.parquet')
    dashboard.add_relation(
        'nation', relation_address='../data/tpch_10mb/nation.parquet')
    dashboard.add_relation(
        'supplier', relation_address='../data/tpch_10mb/supplier.parquet')
    dashboard.add_relation(
        'region', relation_address='../data/tpch_10mb/region.parquet')
    dashboard.add_relation(
        'customer', relation_address='../data/tpch_10mb/customer.parquet')

    dashboard.add_join('lineitem', 'orders', ['l_orderkey'], ['o_orderkey'])
    dashboard.add_join('orders', 'customer', ['o_custkey'], ['c_custkey'])
    dashboard.add_join('partsupp', 'supplier', ['ps_suppkey'], ['s_suppkey'])
    dashboard.add_join('customer', 'nation', ['c_nationkey'], ['n_nationkey'])
    dashboard.add_join('nation', 'region', ['n_regionkey'], ['r_regionkey'])
    dashboard.add_join('lineitem', 'partsupp', ['l_suppkey', 'l_partkey'], [
                       'ps_suppkey', 'ps_partkey'])
    dashboard.add_join('partsupp', 'part', ['ps_partkey'], ['p_partkey'])
    dashboard.register_measurement(
        "sum", 'part', 'p_retailprice', scope=ReplicateFact('part', 'part'))
    dashboard.register_measurement("sum", 'lineitem', 'l_extendedprice * (1 - l_discount)',
                                   scope=ReplicateFact('lineitem', 'lineitem'))
    return dashboard

def generate_synthea_dashboard():
    duck_db_conn = duckdb.connect(database=':memory:')
    join_graph = JoinGraph(duck_db_conn)
    dashboard = DashBoard(join_graph)
    # dashboard.add_relation('allergies', relation_address='../data/synthea/allergies.csv')
    # dashboard.add_relation('careplans', relation_address='../data/synthea/careplans.csv')
    # dashboard.add_relation('claims_transactions', relation_address='../data/synthea/claims_transactions.csv')
    # dashboard.add_relation('claims', relation_address='../data/synthea/claims.csv')
    # dashboard.add_relation('conditions', relation_address='../data/synthea/conditions.csv')
    # dashboard.add_relation('devices', relation_address='../data/synthea/devices.csv')
    dashboard.add_relation('encounters', relation_address='../data/synthea/encounters.csv')
    dashboard.add_relation('imaging_studies', relation_address='../data/synthea/imaging_studies.csv')
    # dashboard.add_relation('immunizations', relation_address='../data/synthea/immunizations.csv')
    dashboard.add_relation('medications', relation_address='../data/synthea/medications.csv')
    # dashboard.add_relation('observations', relation_address='../data/synthea/observations.csv')
    dashboard.add_relation('organizations', relation_address='../data/synthea/organizations.csv')
    dashboard.add_relation('patients', relation_address='../data/synthea/patients.csv')
    # dashboard.add_relation('payer_transitions', relation_address='../data/synthea/payer_transitions.csv')
    dashboard.add_relation('payers', relation_address='../data/synthea/payers.csv')
    dashboard.add_relation('procedures', relation_address='../data/synthea/procedures.csv')
    dashboard.add_relation('providers', relation_address='../data/synthea/providers.csv')
    # dashboard.add_relation('supplies', relation_address='../data/synthea/supplies.csv')

    # dashboard.add_join('allergies', 'patients', ['PATIENT'], ['Id']);
    # dashboard.add_join('careplans', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('procedures', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('providers', 'organizations', ['ORGANIZATION'], ['Id']);
    # dashboard.add_join('supplies', 'patients', ['PATIENT'], ['Id']);
    # dashboard.add_join('observations', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('encounters', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('encounters', 'organizations', ['ORGANIZATION'], ['Id']);
    dashboard.add_join('encounters', 'providers', ['PROVIDER'], ['Id']);
    dashboard.add_join('encounters', 'payers', ['PAYER'], ['Id']);
    dashboard.add_join('imaging_studies', 'encounters', ['ENCOUNTER'], ['Id']);
    # dashboard.add_join('allergies', 'encounters', ['ENCOUNTER'], ['Id']);
    # dashboard.add_join('observations', 'encounters', ['ENCOUNTER'], ['Id']);
    dashboard.add_join('procedures', 'encounters', ['ENCOUNTER'], ['Id']);
    # dashboard.add_join('careplans', 'encounters', ['ENCOUNTER'], ['Id']);
    dashboard.add_join('imaging_studies', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('imaging_studies', 'procedures', ['PROCEDURE_CODE'], ['CODE']);
    # dashboard.add_join('claims_transactions', 'providers', ['PROVIDERID'], ['Id']);
    # dashboard.add_join('claims_transactions', 'patients', ['PATIENTID'], ['Id']);
    # dashboard.add_join('conditions', 'encounters', ['ENCOUNTER'], ['Id']);
    # dashboard.add_join('conditions', 'patients', ['PATIENT'], ['Id']);
    # dashboard.add_join('devices', 'encounters', ['ENCOUNTER'], ['Id']);
    # dashboard.add_join('devices', 'patients', ['PATIENT'], ['Id']);
    # dashboard.add_join('immunizations', 'encounters', ['ENCOUNTER'], ['Id']);
    # dashboard.add_join('immunizations', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('medications', 'encounters', ['ENCOUNTER'], ['Id']);
    dashboard.add_join('medications', 'patients', ['PATIENT'], ['Id']);
    dashboard.add_join('medications', 'payers', ['PAYER'], ['Id']);
    # dashboard.add_join('claims', 'patients', ['PATIENTID'], ['Id']);
    # dashboard.add_join('claims', 'providers', ['PROVIDERID'], ['Id']);
    # dashboard.add_join('payer_transitions', 'patients', ['PATIENT'], ['Id']);
    # dashboard.add_join('payer_transitions', 'payers', ['PAYER'], ['Id']);
    dashboard.register_measurement(
        "sum", 'patients', 'income', scope=ReplicateFact('patients', 'patients'))
    return dashboard

dashboard = generate_synthea_dashboard()

@app.route('/')
def homepage():
    return render_template('index.html')


@app.route('/add_measurement', methods=['POST'])
def add_measurement():
    measurement = request.get_json()
    scope_input = measurement['scope']
    if scope_input == 'ReplicateFact':
        scope = ReplicateFact(measurement['relation'], measurement['fact'])
    elif scope_input == 'FullJoin':
        scope = FullJoin()
    elif scope_input == 'Single':
        scope = SingleRelation(measurement['relation'])

    try:
        dashboard.register_measurement(measurement['agg'].lower(), measurement['relation'].lower(), measurement['attr'].lower(),
                                       scope=scope)
    except Exception as e:
        return jsonify({'error': str(e)})
    return jsonify({'success': True})


@app.route('/get_relation_sample', methods=['POST'])
def get_relation_sample():
    # Get the JSON data from the request
    data = request.get_json()
    print(data)

    # Get the relation from the data
    relation = data["relation"]

    # Get the aggregate expressions from the data, if present
    agg_exprs = data.get("agg_exprs", None)

    print(agg_exprs)

    # If aggregate expressions are present, convert them from text to python enums
    if agg_exprs is not None:
        for k, v in agg_exprs.items():
            agg_exprs[k] = (v[0], Aggregator[v[1]])

    selection_conds = data.get("selection_conds")
    groupby_conds = data.get("groupby_conds")
    orderby_conds = data.get("orderby_conds")
    custom_order_pref = data.get("custom_order_pref")
    limit = data.get("limit", None)

    # Return the sample data
    data = dashboard.get_relation_sample(relation, selection_conds, groupby_conds,
                                        orderby_conds, agg_exprs, limit, custom_order_pref)
    replace_nans(data)
    return jsonify(data)


def replace_nans(data):
    for i in range(len(data['data'])):
        for j, elem in enumerate(data['data'][i]):
            # nan, nattypes are not equal to each other in Python.
            if elem != elem:
                data['data'][i][j] = None


'''
This function returns the graph from dashboard in the format of:
node structure:
nodes: [
    { 
        id: relation,
        attributes: [dim1, dim2, join_key1, join_key2, measurement1, measurement2],
        join_keys: [
            {
                key: col1
                multiplicity: many/one
            },
        ],
        measurements: [ 
        {
            name: AVG(A,..),
            relations: [
            {name: t1, should_highlight: True/False, color: None},
            {name: t2, should_highlight: True/False, color: None}],
            edges: [
            {left_rel: t1, right_rel: t2, should_highlight: True/False, color: None},
            ]
        }, 
        {..}
        ]
    }
]

edge structure: [
    {
        source: node_id,
        left_keys: [key1, key2, ...],
        dest: node_id,
        right_keys: [key1, key2, ...],
    }
]

Edge and relation also stores:
        highlight: true/false (control the opacity)
        color: black by default
These two can be updated in js function through interaction
'''


@app.route('/get_graph')
def get_graph():
    nodes, edges = dashboard.get_graph()
    return jsonify({'nodes': nodes, 'links': edges})
