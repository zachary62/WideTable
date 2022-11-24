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
        # {keys: (from_keys, to_keys), message_type: "", message: name, 
        # multiplicity: x, missing_keys: x ...}
        self.joins = copy.deepcopy(joins)

        # from_relation => to_relation => max multiplicity number
        self.multiplicity = dict()
        self.missing_keys = copy.deepcopy(joins)

        # maps each relation => attribute => dimension attributes (may not include join keys)
        self.relation_schema = copy.deepcopy(relation_schema)

        # some magic/random number used for jupyter notebook display
        self.session_id = int(time.time())
        
        # template for jupyter notebook display
        self.rep_template = data = pkgutil.get_data(__name__, "d3graph.html").decode('utf-8')
        
    # return a list of relations in the join graph 
    def get_relations(self):
        return list(self.relation_schema.keys())
    
    # return whether the relation is in the join graph 
    def has_relation(self, relation):
        return (relation in self.relation_schema.keys())
    
    def get_relation_schema(self):
        return self.relation_schema

    def get_relation_attributes(self, relation):
        return list(self.relation_schema[relation].keys())
    
    def get_type(self, relation, attribute): 
        return self.relation_schema[relation][attribute]

    def get_joins(self):
        return self.joins
    
    # check if the join graph is acyclic and not disjoint
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
        self.multiplicity[relation] = dict()
        self.missing_keys[relation] = dict()

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

    def get_multiplicity(self, from_table, to_table):
        return self.joins[from_table][to_table]["multiplicity"]
    
    def get_missing_keys(self, from_table, to_table):
        return self.joins[from_table][to_table]["missing_keys"]

    # get the dimension attributes and the join keys
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
        self.determine_multiplicity_and_missing(table_name_left, left_keys, table_name_right, right_keys)
        
    def get_num_missing_join_keys(self, 
                                  table_name_left :str, 
                                  leftKeys: list,
                                  table_name_right:str, 
                                  rightKeys: list):
        # below two queries get the set of join keys
        set_left = self.exe.execute_spja_query(aggregate_expressions={"join_key": (",".join(leftKeys), Aggregator.IDENTITY)},
                                         from_tables=[table_name_left], 
                                         mode=4)
        set_right = self.exe.execute_spja_query(aggregate_expressions={"join_key": (",".join(rightKeys), Aggregator.IDENTITY)},
                                         from_tables=[table_name_right], 
                                         mode=4)
        
        # below two queries get the difference of join keys
        diff_left = self.exe.set_query("EXCEPT", set_left, set_right)
        diff_right = self.exe.set_query("EXCEPT", set_right, set_left)
        
        # get the count of the difference of join keys
        num_miss_left = self.exe.execute_spja_query(aggregate_expressions={'count':('*',  Aggregator.COUNT)},
                                                    from_tables=[diff_left],
                                                    mode=3)[0][0]
        
        num_miss_right = self.exe.execute_spja_query(aggregate_expressions={'count':('*',  Aggregator.COUNT)},
                                                    from_tables=[diff_right],
                                                    mode=3)[0][0]
        
        return num_miss_left, num_miss_right
        

    def determine_multiplicity_and_missing(self, 
                                          table_name_left :str, 
                                          leftKeys: list,
                                          table_name_right:str, 
                                          rightKeys: list):
        
        num_miss_left, num_miss_right = self.get_num_missing_join_keys(table_name_left, 
                                                                       leftKeys, 
                                                                       table_name_right, 
                                                                       rightKeys)
        
        self.joins[table_name_right][table_name_left]["missing_keys"] = num_miss_left
        self.joins[table_name_left][table_name_right]["missing_keys"] = num_miss_right

        self.joins[table_name_left][table_name_right]["multiplicity"] = \
        self.get_max_multiplicity(table_name_left, leftKeys)
        self.joins[table_name_right][table_name_left]["multiplicity"] = \
        self.get_max_multiplicity(table_name_right, rightKeys)

    def get_max_multiplicity(self, table, keys):
        multiplicity = self.exe.execute_spja_query(aggregate_expressions={'count': ('*',  Aggregator.COUNT)},
                                                   from_tables=[table],
                                                   group_by=keys,
                                                   mode=4)
        
        max_multiplicity=self.exe.execute_spja_query(aggregate_expressions={'max_count': ('count', Aggregator.MAX)},
                                                     from_tables=['(' + multiplicity + ')'],
                                                     mode=3)[0][0]
        return max_multiplicity
    
    # replace the table name from table_prev to table_after
    def replace(self, table_prev, table_after):
        if table_prev not in self.relation_schema:
            raise JoinGraphException(table_prev + ' doesn\'t exist!')
        if table_after in self.relation_schema:
            raise JoinGraphException(table_after + ' already exists!')
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
            attributes = set(self.get_useful_attributes(table_name))
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