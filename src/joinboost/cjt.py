import copy
from .semiring import SemiRing
from .joingraph import JoinGraph
from .aggregator import *


class CJT(JoinGraph):
    def __init__(self, 
                 semi_ring: SemiRing, 
                 join_graph: JoinGraph, 
                 annotations: dict = {}):
        self.message_id = 0
        self.semi_ring = semi_ring
        super().__init__(join_graph.exe,
                         join_graph.joins, 
                         join_graph.relation_schema,
                         join_graph.target_var,
                         join_graph.target_relation)
        
        # maps relation to a set of annotations
        self.annotations = annotations
    
    # given the from_table and to_table, return the message in between
    def get_message(self, from_table: str, to_table: str):
        return self.joins[from_table][to_table]['message']

    def get_parsed_annotations(self, table):
        if table not in self.annotations:
            return []
        return parse_ann({table: self.annotations[table]})

    def get_all_parsed_annotations(self):
        # zach: I don't remember why the "True" for prepend. maybe we can remove it
        return parse_ann(self.annotations, True)

    def add_annotations(self, r_name: str, annotation: str, lazy=True):
        # TODO: add some check for annotation. E.g., is the referenced attribute even in the relation?
        if r_name not in self.annotations:
            self.annotations[r_name] = [annotation]
        else:
            self.annotations[r_name].append(annotation)
        # TODO: after add annotation, all messages from this relation are invalidated. 
        # if lazy is false, invalivate messages

    # TODO: this function takes the from_table, to_table,
    # and remove the message (delete table + remove it from the self.joins)
    # if lazy, don't delete table in databases
    def invalidate_message(self, from_table, to_table, lazy):
        pass
    
    # TODO: check relation to leave is a leaf node in the join graph.
    def remove_table(self, relation):
        pass
    
    # TODO: check relation_join_with_the_new_reltion is a leaf node.
    # if it is, add the new_relation to join graph
    # invalidate messages from relation_join_with_the_new_reltion
    # Of course better variable names
    def add_table(self, new_relation, relation_join_with_the_new_reltion, join_keys):
        pass
    
    # TODO: use "invalidate_message" function to remove message
    def clean_message(self):
        for from_table in self.joins:
            for to_table in self.joins[from_table]:
                if self.joins[from_table][to_table]['message_type'] != Message.IDENTITY:
                    m_name = self.joins[from_table][to_table]['message']
                    self.exe.delete_table(m_name)

    def get_semi_ring(self): 
        return self.semi_ring
    
    # this is for "what-if query"
    # copy a new cjt so the old are kept
    def copy_cjt(self, semi_ring: SemiRing):
        annotations = copy.deepcopy(self.annotations)
        c_cjt = CJT(semi_ring=semi_ring, 
                    join_graph=self, 
                    annotations=annotations)
        return c_cjt

    def get_root_neighbors(self):
        neighbors = {}
        for table in self.joins[self.target_relation]:
            if self.joins[table][self.target_relation]['message_type'] != Message.IDENTITY:
                neighbors[table] = (self.get_join_keys(self.target_relation, table),
                                    self.get_message(table, self.target_relation))
        return neighbors
    
    def calibration(self, root_table: str = None):
        # TODO: choose the first relation in the joins
        if not root_table:
            # currently below doesn't pass test. check why
            # root_table = list(self.joins.keys())[0]
            root_table = self.target_relation
        self.upward_message_passing(root_table, m_type=Message.FULL)
        self.downward_message_passing(root_table, m_type=Message.FULL)
        
    # TODO: for both upward and downward message passing, check if it current exist, and skip if yes.
    def downward_message_passing(self, 
                                 rooto_table: str = None, 
                                 m_type: Message = Message.UNDECIDED):
        msgs = []
        if not rooto_table:
            rooto_table = self.target_relation
        self._pre_dfs(rooto_table, m_type=m_type)
        return msgs
    
    # TODO: this is not working if upward_message_passing from non-fact table
    #  Above TODO needs to be rechecked
    def upward_message_passing(self, root_table: str = None,
                               m_type: Message = Message.UNDECIDED):
        if not root_table:
            root_table = self.target_relation
        self._post_dfs(root_table, m_type=m_type)

    def _post_dfs(self, currento_table: str, 
                  parent_table: str = None, 
                  m_type: Message = Message.UNDECIDED):
        jg = self.get_joins()
        if currento_table not in jg:
            return
        for c_neighbor in jg[currento_table]:
            if c_neighbor != parent_table:
                self._post_dfs(c_neighbor, currento_table, m_type=m_type)
        if parent_table:
            self._send_message(from_table=currento_table, to_table=parent_table, m_type=m_type)

    def _pre_dfs(self, currento_table: str, 
                 parent_table: str = None, 
                 m_type: Message = Message.UNDECIDED):
        joins = self.get_joins()
        if currento_table not in joins:
            return
        if currento_table == self.target_relation:
            m_type = Message.FULL
        for c_neighbor in joins[currento_table]:
            if c_neighbor != parent_table:
                self._send_message(currento_table, c_neighbor, m_type=m_type)
                self._pre_dfs(c_neighbor, currento_table, m_type=m_type)

    def absorption(self, table: str, group_by: list, mode=4):
        # from_table_attrs = self.get_relation_features(table)
        incoming_messages, join_conds = self._get_income_messages(table)

        from_relations = [m['message'] for m in incoming_messages] + [table]
        aggregate_expressions = self.semi_ring.col_product_sum(from_relations)
        for attr in group_by:
            aggregate_expressions[attr] = (table + "." + attr, Aggregator.IDENTITY)

        return self.exe.execute_spja_query(aggregate_expressions, 
                                           from_tables=from_relations,
                                           select_conds=join_conds+self.get_parsed_annotations(table),
                                           group_by=[table + '.' + attr for attr in group_by], 
                                           mode=mode)

    # get the incoming message from one table to another
    # key function for message passing, Sec 3.3 of CJT paper
    # allow two types of join condition: 1 is for selection, 2 is for semi-join
    def _get_income_messages(self, 
                             table: str, 
                             excluded_table: str = ''):
        incoming_messages, join_conds = [], []
        for neighbour_table in self.joins[table]:
            # if neighbour_table != excluded_table:
            incoming_message = self.joins[neighbour_table][table]
            if 'message_type' not in incoming_message:
                continue
            if 'message_type' in incoming_message and incoming_message['message_type'] == Message.IDENTITY:
                continue

            # get the join conditions between from_table and incoming_message
            l_join_keys, r_join_keys = self.get_join_keys(neighbour_table, table)
            incoming_messages.append(incoming_message)
            
            join_conds += [incoming_message["message"] + "." + l_join_keys[i] + " IS NOT DISTINCT FROM " +
                           table + "." + r_join_keys[i] for i in range(len(l_join_keys))]
                
        return incoming_messages, join_conds

    # 3 message types: identity, selection, FULL
    def _send_message(self, from_table: str, to_table: str, m_type: Message = Message.FULL):
        print('--Sending Message from', from_table, 'to', to_table, 'm_type is', m_type)
        # identity message optimization
        if m_type == Message.IDENTITY:
            self.joins[from_table][to_table].update({'message_type': m_type,})
            return
        
        if from_table not in self.joins and to_table not in self.joins[from_table]:
            raise Exception('Table', from_table, 'and table', to_table, 'are not connected')
        
        # join with incoming messages
        incoming_messages, join_conds = self._get_income_messages(from_table, to_table)
        
        if m_type == Message.UNDECIDED:
            m_type = Message.FULL

        # get the group_by key for this message
        l_join_keys, _ = self.get_join_keys(from_table, to_table)
        from_relations = [m['message'] for m in incoming_messages] + [from_table]
        # compute aggregation
        aggregation = (self.semi_ring.col_product_sum(relations=from_relations) if m_type == Message.FULL else {})
        
        for attr in l_join_keys:
            aggregation[attr] = (from_table + "." + attr, Aggregator.IDENTITY)
            
        message_name = self.exe.execute_spja_query(aggregation, 
                                                    from_tables=from_relations,
                                                    select_conds=join_conds+self.get_parsed_annotations(from_table),
                                                    group_by=[from_table + '.' + attr for attr in l_join_keys], 
                                                    mode=1)

        self.joins[from_table][to_table].update({'message': message_name, 'message_type': m_type})
    
    # by default, lift the target attribute
    # Essentially, this method renames the relevant attributes
    # TODO: the attr is semi-ring specific. E.g., count semi-ring doesn't even need attr. 
    # Make it a para to semi-ring
    def lift(self, relation=None, attr=None):
        if relation is None:
            relation = self.target_relation
        if attr is None:
            attr = self.target_var
        lift_exp = self.semi_ring.lift_exp(relation + '.' + attr)
        # copy the remaining attributes as they are (no aggregation)
        for attr in self.get_useful_attributes(relation):
            lift_exp[attr] = (attr, Aggregator.IDENTITY)
        new_fact_name = self.exe.execute_spja_query(lift_exp,
                                                    [relation],
                                                    mode=1)
        self.replace(relation, new_fact_name)
