import time

from .aggregator import *
import copy
from .executor import ExecutorFactory
import pkgutil


class JoinGraphException(Exception):
    pass


class JoinGraph:
    def __init__(self,
                 exe=None,
                 joins={},
                 relation_info={}):

        self.exe = ExecutorFactory(exe)

        # maps each from_relation => to_relation =>
        # {keys: (from_keys, to_keys), message_type: "", message: name,
        # multiplicity: x, missing_keys: x ...}
        self.joins = copy.deepcopy(joins)

        # maps each relation => "schema" => attribute_name => dimension attributes (may not include join keys)
        #                    => "user_table" => user_table
        # user_table is what user call the table. We can rename the table for, e.g., for lift.
        self.relation_info = copy.deepcopy(relation_info)

        # some magic/random number used for jupyter notebook display
        self.session_id = int(time.time())

        # template for jupyter notebook display
        self.rep_template = data = pkgutil.get_data(
            __name__, "static_files/joingraph.html").decode('utf-8')

    # return a list of relations in the join graph
    def get_relations(self):
        return list(self.relation_info.keys())

    def get_relation_sample(self, relation, select_conds, limit=100):
        if relation not in self.get_relations():
            raise JoinGraphException("The relation doesn't exist!")

        # get data as pandas because we all want to have the header
        df = self.exe.execute_spja_query(from_tables=[relation],
                                         select_conds=select_conds,
                                         limit=limit,
                                         mode=5)
        return {"header": df.columns.tolist(), "data": df.values.tolist()}

    # return whether the relation is in the join graph
    def has_relation(self, relation):
        return (relation in self.relation_info.keys())

    def get_relation_info(self):
        return self.relation_info

    def get_relation_attributes(self, relation):
        return list(self.relation_info[relation]["schema"].keys())

    def get_relations_from_attribute(self, attribute):
        relations = []
        for relation in self.relation_info:
            if attribute in self.relation_info[relation]["schema"].keys():
                relations.append(relation)
        return relations

    def get_type(self, relation, attribute):
        return self.relation_info[relation]["schema"][attribute]

    def get_user_table(self, relation):
        return self.relation_info[relation]["user_table"]

    # given a user_table, find the table name in the database
    def get_relation_from_user_table(self, user_table):
        for relation in self.relation_info:
            if user_table == self.relation_info[relation]["user_table"]:
                return relation
        return None

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
                     attrs: list = None,
                     relation_address=None):

        self.exe.add_table(relation, relation_address)
        self.joins[relation] = dict()

        if relation not in self.relation_info:
            self.relation_info[relation] = {}
            self.relation_info[relation]["schema"] = {}
            self.relation_info[relation]["user_table"] = relation

        if attrs is None:
            attrs = self.exe.get_schema(relation)

        for x in attrs:
            self.relation_info[relation]["schema"][x] = ""

    # get the join keys between two tables
    # or if t_table is None, get all the join keys of one table
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

    def get_multiplicity(self, from_table, to_table, simple=False):
        if not simple:
            return self.joins[from_table][to_table]["multiplicity"]
        else:
            return "M" if (self.joins[from_table][to_table]["multiplicity"] > 1) else "1"

    def get_missing_keys(self, from_table, to_table):
        return self.joins[from_table][to_table]["missing_keys"]

    # get the dimension attributes and the join keys
    def get_useful_attributes(self, table):
        useful_attributes = self.get_relation_attributes(table) + \
            self.get_join_keys(table)
        return list(set(useful_attributes))

    def add_join(self, relation_left: str, relation_right: str, left_keys: list, right_keys: list):
        if len(left_keys) != len(right_keys):
            raise JoinGraphException('Join keys have different lengths!')
        if relation_left not in self.relation_info:
            raise JoinGraphException(relation_left + ' doesn\'t exit!')
        if relation_right not in self.relation_info:
            raise JoinGraphException(relation_right + ' doesn\'t exit!')

        left_keys = [attr for attr in left_keys]
        right_keys = [attr for attr in right_keys]

        self.joins[relation_left][relation_right] = {"keys": (left_keys, right_keys),
                                                     "message_type": Message.UNDECIDED, }
        self.joins[relation_right][relation_left] = {"keys": (right_keys, left_keys),
                                                     "message_type": Message.UNDECIDED, }
        self.determine_multiplicity_and_missing(
            relation_left, left_keys, relation_right, right_keys)

    def get_num_missing_join_keys(self,
                                  relation_left: str,
                                  leftKeys: list,
                                  relation_right: str,
                                  rightKeys: list):
        # below two queries get the set of join keys
        set_left = self.exe.execute_spja_query(aggregate_expressions={"join_key": (",".join(leftKeys), Aggregator.IDENTITY)},
                                               from_tables=[relation_left],
                                               mode=4)
        set_right = self.exe.execute_spja_query(aggregate_expressions={"join_key": (",".join(rightKeys), Aggregator.IDENTITY)},
                                                from_tables=[relation_right],
                                                mode=4)

        # below two queries get the difference of join keys
        diff_left = self.exe.set_query("EXCEPT", set_left, set_right)
        diff_right = self.exe.set_query("EXCEPT", set_right, set_left)

        # get the count of the difference of join keys
        num_miss_left = self.exe.execute_spja_query(aggregate_expressions={'count': ('*',  Aggregator.COUNT)},
                                                    from_tables=[diff_left],
                                                    mode=3)[0][0]

        num_miss_right = self.exe.execute_spja_query(aggregate_expressions={'count': ('*',  Aggregator.COUNT)},
                                                     from_tables=[diff_right],
                                                     mode=3)[0][0]

        return num_miss_left, num_miss_right

    def determine_multiplicity_and_missing(self,
                                           relation_left: str,
                                           leftKeys: list,
                                           relation_right: str,
                                           rightKeys: list):

        num_miss_left, num_miss_right = self.get_num_missing_join_keys(relation_left,
                                                                       leftKeys,
                                                                       relation_right,
                                                                       rightKeys)

        self.joins[relation_right][relation_left]["missing_keys"] = num_miss_left
        self.joins[relation_left][relation_right]["missing_keys"] = num_miss_right

        self.joins[relation_left][relation_right]["multiplicity"] = \
            self.get_max_multiplicity(relation_left, leftKeys)
        self.joins[relation_right][relation_left]["multiplicity"] = \
            self.get_max_multiplicity(relation_right, rightKeys)

    def get_max_multiplicity(self, table, keys):
        multiplicity = self.exe.execute_spja_query(aggregate_expressions={'count': ('*',  Aggregator.COUNT)},
                                                   from_tables=[table],
                                                   group_by=keys,
                                                   mode=4)

        max_multiplicity = self.exe.execute_spja_query(aggregate_expressions={'max_count': ('count', Aggregator.MAX)},
                                                       from_tables=[
            '(' + multiplicity + ')'],
            mode=3)[0][0]
        return max_multiplicity

    # replace the table name from table_prev to table_after
    def replace(self, table_prev, table_after):
        if table_prev not in self.relation_info:
            raise JoinGraphException(table_prev + ' doesn\'t exist!')
        if table_after in self.relation_info:
            raise JoinGraphException(table_after + ' already exists!')
        self.relation_info[table_after] = self.relation_info[table_prev]
        # is it safe? do we need a deep copy?
        del self.relation_info[table_prev]

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
        for table in self.relation_info:
            features = self.relation_info[table]["schema"].keys()
            self.check_features_exist(table, features)

    def check_features_exist(self, table, features):
        attributes = self.exe.get_schema(table)
        if not set(features).issubset(set(attributes)):
            raise JoinGraphException('Key error in ' + str(features)
                                     + '. Attribute does not exist in table '
                                     + table + ' with schema ' + str(attributes))

    # output html that displays the join graph. Taken from JoinExplorer notebook
    def _repr_html_(self):
        nodes = []
        links = []
        for relation in self.get_relations():
            attributes = set(self.get_useful_attributes(relation))
            nodes.append({"id": relation, "attributes": list(attributes)})

        seen = set()
        for relation_left in self.joins:
            for relation_right in self.joins[relation_left]:
                # avoid edge in opposite direction
                if (relation_right, relation_left) in seen:
                    continue
                keys = self.joins[relation_left][relation_right]['keys']
                links.append({"source": relation_left, "target": relation_right,
                              "left_keys": keys[0], "right_keys": keys[1]})
                seen.add((relation_left, relation_right))

        self.session_id += 1

        s = self.rep_template
        s = s.replace("{{session_id}}", str(self.session_id))
        s = s.replace("{{nodes}}", str(nodes))
        s = s.replace("{{links}}", str(links))
        return s
