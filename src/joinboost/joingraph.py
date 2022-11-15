import time

from .aggregator import Aggregator
import copy
from .executor import ExecutorFactory
import pkgutil

class JoinGraphException(Exception):
    pass

class JoinGraph:
    def __init__(self,
                exe = None,
                joins = {},
                relation_schema = {}):

        self.exe = ExecutorFactory(exe)
        # maps each from_relation => to_relation => 
        # {keys: (from_keys, to_keys), message_type: "", message: name, ...}
        self.joins = copy.deepcopy(joins)

        # from_relation => to_relation => "M_to_O"/"O_to_M"/"M_to_M"/"O_to_O"
        self.cardinality = copy.deepcopy(joins)

        # maps each relation => attribute => feature_type
        self.relation_schema = copy.deepcopy(relation_schema)

        # some magic/random number used for jupyter notebook display
        self.session_id = int(time.time())

        self.rep_template = data = pkgutil.get_data(__name__, "d3graph.html").decode('utf-8')

    def get_relations(self):
        return list(self.relation_schema.keys())

    def get_relation_schema(self):
        return self.relation_schema

    def get_relation_attributes(self, relation):
        return list(self.relation_schema[relation].keys())
    
    def get_type(self, relation, attribute): 
        return self.relation_schema[relation][attribute]

    def get_joins(self):
        return self.joins

    def check_acyclic(self):
        seen = set()

        def dfs(cur_table, parent=None):
            seen.add(cur_table)
            for neighbour in self.joins[cur_table]:
                if neighbour != parent:
                    if neighbour in seen:
                        return False
                    else:
                        dfs(neighbour, cur_table)
            return True

        # check acyclic
        if not dfs(list(self.joins.keys())[0]):
            raise JoinGraphException("The join graph is cyclic!")

        # check not disjoint
        if len(seen) != len(self.joins):
            raise JoinGraphException("The join graph is disjoint!")

    # add relation and attributes to join graph
    def add_relation(self,
                     relation: str,
                     attrs: list = [],
                     relation_address = None):

        self.exe.add_table(relation, relation_address)
        self.joins[relation] = dict()
        self.cardinality[relation] = dict()

        if relation not in self.relation_schema:
            self.relation_schema[relation] = {}

        for x in attrs:
            self.relation_schema[relation][x] = ""

    # get the join keys between two tables
    # all get all the join keys of one table
    # TODO: check if the join keys exist
    def get_join_keys(self, f_table: str, t_table: str = None):
        if f_table not in self.joins:
            return []

        if t_table:
            if t_table not in self.joins[f_table]:
                raise JoinGraphException(t_table, 'not connected to', f_table)
            return self.joins[f_table][t_table]["keys"]
        else:
            keys = set()
            for table in self.joins[f_table]:
                l_keys, _ = self.joins[f_table][table]["keys"]
                keys = keys.union(set(l_keys))
            return list(keys)

    # useful attributes are join keys
    def get_useful_attributes(self, table):
        useful_attributes = self.get_relation_attributes(table) + \
                            self.get_join_keys(table)
        return list(set(useful_attributes))

    def add_join(self, table_name_left: str, table_name_right: str, left_keys: list, right_keys: list):
        if len(left_keys) != len(right_keys):
            raise JoinGraphException('Join keys have different lengths!')
        if table_name_left not in self.relation_schema:
            raise JoinGraphException(table_name_left + ' doesn\'t exit!')
        if table_name_right not in self.relation_schema:
            raise JoinGraphException(table_name_right + ' doesn\'t exit!')

        left_keys = [attr for attr in left_keys]
        right_keys = [attr for attr in right_keys]

        self.joins[table_name_left][table_name_right] = {"keys": (left_keys, right_keys)}
        self.joins[table_name_right][table_name_left] = {"keys": (right_keys, left_keys)}
        self.determine_cardinality(table_name_left, left_keys, table_name_right, right_keys)

    def determine_cardinality(self, table_name_left :str, leftKeys: list,
                              table_name_right:str, rightKeys: list):
        max_rcount = self.execute_cardinality_query(table_name_left,leftKeys,
                                                table_name_right,rightKeys)
        max_lcount = self.execute_cardinality_query(table_name_right, rightKeys,
                                                table_name_left, leftKeys)

        if max_lcount > 1 and max_rcount > 1:
            self.cardinality[table_name_left][table_name_right] = "M_to_M"
            self.cardinality[table_name_right][table_name_left] = "M_to_M"
        elif max_lcount == 0 or max_rcount == 0:
            self.cardinality[table_name_left][table_name_right] = "MISSING"
            self.cardinality[table_name_right][table_name_left] = "MISSING"
        elif max_lcount == 1 and max_rcount == 1:
            self.cardinality[table_name_left][table_name_right] = "O_to_O"
            self.cardinality[table_name_right][table_name_left] = "O_to_O"
        elif max_lcount > 1 and max_rcount == 1:
            self.cardinality[table_name_left][table_name_right] = "M_to_O"
            self.cardinality[table_name_right][table_name_left] = "O_to_M"
        elif max_rcount > 1 and max_lcount == 1:
            self.cardinality[table_name_left][table_name_right] = "O_to_M"
            self.cardinality[table_name_right][table_name_left] = "M_to_O"

    def execute_cardinality_query(self, ltable, lkeys, rtable, rkeys):
        agg_exp = {}
        for lkey in lkeys:
            agg_exp[lkey] = (f'{ltable}.{lkey}', Aggregator.IDENTITY)
        join_conds=[ltable + "." + lkeys[i] + " IS NOT DISTINCT FROM " +
         rtable + "." + rkeys[i] for i in range(len(lkeys))]
        left_distinct_table = self.exe.execute_spja_query(aggregate_expressions=agg_exp,
                                                          from_tables=[ltable],
                                                          group_by=lkeys,
                                                          table_name=ltable,
                                                          mode=4)

        agg_exp['count'] = (','.join([f'{rtable}.{rkey}' for rkey in rkeys]), Aggregator.COUNT)
        res=self.exe.execute_spja_query(from_tables=[left_distinct_table + f'as {ltable}', rtable],
                                        group_by=[f'{ltable}.{lkey}' for lkey in lkeys],
                                        select_conds=join_conds,
                                        aggregate_expressions=agg_exp,
                                        mode=3
                                        )
        max_count = 0
        for t in res:
            max_count = max(t[1], max_count)
        return max_count

    def replace(self, table_prev, table_after):
        if table_prev not in self.relation_schema:
            raise JoinGraphException(table_prev + ' doesn\'t exit!')
        if table_after in self.relation_schema:
            raise JoinGraphException(table_after + ' already exits!')
        self.relation_schema[table_after] = self.relation_schema[table_prev]
        del self.relation_schema[table_prev]

        for relation in self.joins:
            if table_prev in self.joins[relation]:
                self.joins[relation][table_after] = self.joins[relation][table_prev]
                del self.joins[relation][table_prev]

        if table_prev in self.joins:
            self.joins[table_after] = self.joins[table_prev]
            del self.joins[table_prev]

    def _preprocess(self):
        # self.check_all_features_exist()
        self.check_acyclic()

    def check_all_features_exist(self):
        for table in self.relation_schema:
            features = self.relation_schema[table].keys()
            self.check_features_exist(table, features)

    def check_features_exist(self, table, features):
        attributes = self.exe.get_schema(table)
        if not set(features).issubset(set(attributes)):
            raise JoinGraphException('Key error in ' + str(features) \
                                     + '. Attribute does not exist in table ' \
                                     + table + ' with schema ' + str(attributes))

    # output html that displays the join graph. Taken from JoinExplorer notebook
    def _repr_html_(self):
        nodes = []
        links = []
        for table_name in self.relation_schema:
            attributes = set(self.exe.get_schema(table_name))
            nodes.append({"id": table_name, "attributes": list(attributes)})

        # avoid edge in opposite direction
        seen = set()
        for table_name_left in self.joins:
            for table_name_right in self.joins[table_name_left]:
                if (table_name_right, table_name_left) in seen:
                    continue
                keys = self.joins[table_name_left][table_name_right]['keys']
                links.append({"source": table_name_left, "target": table_name_right, \
                              "left_keys": keys[0], "right_keys": keys[1]})
                seen.add((table_name_left, table_name_right))

        self.session_id += 1

        s = self.rep_template
        s = s.replace("{{session_id}}", str(self.session_id))
        s = s.replace("{{nodes}}", str(nodes))
        s = s.replace("{{links}}", str(links))
        return s