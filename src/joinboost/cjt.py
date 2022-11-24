import copy
from .semiring import *
from .joingraph import JoinGraph
from .aggregator import *


class CJT(JoinGraph):
    def __init__(self,
                 semi_ring: SemiRing,
                 join_graph: JoinGraph):
        
        self.message_id = 0
        self.semi_ring = semi_ring
        # maps relation to the columns storing semi-ring. e.g., "R" -> {"s": "s", "c": "c"}
        self.semi_ring_cols = {}
        
        super().__init__(join_graph.exe,
                         join_graph.joins,
                         join_graph.relation_info)
        
        # cjt additionally store annotations
        # maps relation to a set of annotations
        # self.relation_info => rel => annotation => [attr, annotation_type, value]
        # for now, treat group-by annotations specially
        # self.relation_info => rel => groupby =>[group attributes]
        # TODO: currently assume that attribute name are unique across relations
        

    # given the from_table and to_table, return the message in between
    def get_message(self, from_table: str, to_table: str):
        return self.joins[from_table][to_table]['message']

    def delete_message(self, from_table: str, to_table: str):
        del self.joins[from_table][to_table]['message']
        self.joins[from_table][to_table]['message_type'] = 'missing'

    def get_parsed_annotations(self, relation):
        if "annotation" not in self.relation_info[relation]:
            return []
        return parse_ann({relation: self.relation_info[relation]["annotation"]})

    def add_annotations(self, user_table: str, annotation, lazy=True):
        # TODO: add some check for annotation. 
        # E.g., is the referenced attribute even in the relation?
        relation = self.get_relation_from_user_table(user_table)
        
        if "annotation" not in self.relation_info[relation]:
            self.relation_info[relation]["annotation"] = [annotation]
        else:
            self.relation_info[relation]["annotation"].append(annotation)

        if not lazy:
            for to_relation,v in self.joins[relation]:
                self.invalidate_message(relation, to_relation)
    
    def remove_annotations(self, relation: str, annotation, lazy=True):
        pass
    
    def add_groupbys(self, user_table, attributes, lazy=True):
        relation = self.get_relation_from_user_table(user_table)
        if "groupby" not in self.relation_info[relation]:
            self.relation_info[relation]["groupby"] = attributes
        else:
            self.relation_info[relation]["groupby"] += attributes

        if not lazy:
            for to_relation,v in self.joins[relation]:
                self.invalidate_message(relation, to_relation)
                
    def remove_groupbys(self, relation, attributes, lazy=True):
        pass

    def invalidate_message(self, from_table, to_table, lazy:bool):

        message_name = self.get_message(from_table, to_table)
        self.delete_message(from_table, to_table)
        if not lazy:
            self.exe.delete_table(message_name)

    # TODO: check relation to leave is a leaf node in the join graph.
    def remove_relation(self, relation):
        pass
    
    # TODO: add the new_relation to join graph.
    # invalidate messages from relation_join_with_the_new_reltion
    # Of course better variable names
    def add_relation(self, new_relation, relation_join_with_the_new_reltion, join_keys):
        pass
    
    # TODO: use "invalidate_message" function to remove message
    def clean_message(self):
        for from_table in self.joins:
            for to_table in self.joins[from_table]:
                if self.joins[from_table][to_table]['message_type'] != Message.IDENTITY:
                    m_name = self.joins[from_table][to_table]['message']
                    self.exe.delete_table(m_name)
                    
    def add_relation(self,
                     relation: str,
                     attrs: list = [],
                     relation_address=None):
        super().add_relation(relation, attrs=attrs, relation_address=relation_address)

    def get_semi_ring(self):
        return self.semi_ring
    
    # this is for "what-if query"
    # copy a new cjt so the old are kept
    def copy_cjt(self, semi_ring: SemiRing):
        pass

    def calibration(self, root_table=None):
        # TODO: choose the first relation in the joins
        if not root_table:
            # currently below doesn't pass test. check why
            root_table = self.get_relations()[0]
#             raise ValueError("root table can not be None")
        self.upward_message_passing(root_table, m_type=Message.FULL)
        self.downward_message_passing(root_table, m_type=Message.FULL)
        
    # TODO: for both upward and downward message passing, check if it current exist, and skip if yes.
    def downward_message_passing(self,
                                 rooto_table: str,
                                 m_type: Message = Message.UNDECIDED):
        msgs = []
        if not rooto_table:
            raise ValueError("root table can not be None")
        self._pre_dfs(rooto_table, m_type=m_type)
        return msgs
    
    def upward_message_passing(self, root_table: str,
                               m_type: Message = Message.UNDECIDED):
        if not root_table:
            raise ValueError("root table can not be None")
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
        for c_neighbor in joins[currento_table]:
            if c_neighbor != parent_table:
                self._send_message(currento_table, c_neighbor, m_type=m_type)
                self._pre_dfs(c_neighbor, currento_table, m_type=m_type)
                
    
    def absorption(self, user_table: str, group_by: list, order_by = [], mode=4):
        relation = self.get_relation_from_user_table(user_table)
        incoming_messages, join_conds = self._get_income_messages(relation)

        from_relations = [m['message'] for m in incoming_messages] + [relation]
        aggregate_expressions = self.semi_ring.compute_col_operations(from_relations)
        
        # TODO: this is ugly. Refactor the codes
        group_by_query = []
        for attr in group_by:
            if attr in self.get_join_keys(relation):
                aggregate_expressions[attr] = (relation + "." + attr, Aggregator.IDENTITY)
                group_by_query.append(relation + '.' + attr)
            else:
                aggregate_expressions[attr] = (attr, Aggregator.IDENTITY)
                group_by_query.append(attr)
                
        order_by_query = []        
        for attr in order_by:
            if attr in self.get_join_keys(relation):
                order_by_query.append(relation + '.' + attr)
            else:
                order_by_query.append(attr)
        
        return self.exe.execute_spja_query(aggregate_expressions,
                                           from_tables=from_relations,
                                           select_conds=join_conds+self.get_parsed_annotations(relation),
                                           group_by=group_by_query,
                                           order_by=order_by_query,
                                           mode=mode)

    # get the incoming message from one table to another
    # key function for message passing, Sec 3.3 of CJT paper
    def _get_income_messages(self,
                             table: str, 
                             excluded_table: str = ''):
        incoming_messages, join_conds = [], []
        for neighbour_table in self.joins[table]:
            if neighbour_table != excluded_table:
                incoming_message = self.joins[neighbour_table][table]
                # there is no incoming message
                # maybe good to report error
                if 'message_type' not in incoming_message:
                    continue
                if 'message_type' in incoming_message and \
                    incoming_message['message_type'] == Message.IDENTITY:
                    continue

                # get the join conditions between from_table and incoming_message
                from_join_keys, r_join_keys = self.get_join_keys(neighbour_table, table)
                incoming_messages.append(incoming_message)

                join_conds += [incoming_message["message"] + "." + from_join_keys[i] + " IS NOT DISTINCT FROM " +
                               table + "." + r_join_keys[i] for i in range(len(from_join_keys))]
                
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
        from_join_keys, _ = self.get_join_keys(from_table, to_table)
        from_relations = [m['message'] for m in incoming_messages] + [from_table]
        from_group_bys = [attr for m in incoming_messages for attr in m['message_groupby']]
        
        if "groupby" in self.relation_info[from_table]:
            from_group_bys += self.relation_info[from_table]["groupby"]
        
        group_by = [from_table + '.' + attr for attr in from_join_keys] \
                    + from_group_bys
        
        # compute aggregation
        aggregation = (self.semi_ring.compute_col_operations(relations=from_relations)
                       if m_type == Message.FULL else {})
        
        # TODO: this is ugly! fix it such that "for attr in group_by" works
        for attr in from_join_keys:
            aggregation[attr] = (from_table + "." + attr, Aggregator.IDENTITY)
        for attr in from_group_bys:
            aggregation[attr] = (attr, Aggregator.IDENTITY)
        

        message_name = self.exe.execute_spja_query(aggregation,
                                                    from_tables=from_relations,
                                                    select_conds=join_conds+self.get_parsed_annotations(from_table),
                                                    group_by=group_by,
                                                    mode=1)

        self.joins[from_table][to_table].update({'message': message_name, 
                                                 'message_type': m_type,
                                                 'message_groupby': from_group_bys})

    def lift(self, relation):
        if relation is None:
            raise ValueError("Invalid relation")
        
        user_table = self.get_user_table(relation)
        lift_exp = self.semi_ring.lift_exp(relation=user_table)
        
        # copy the remaining attributes as they are (no aggregation)
        for attr in self.get_useful_attributes(relation):
            lift_exp[attr] = (attr, Aggregator.IDENTITY)
        new_fact_name = self.exe.execute_spja_query(lift_exp,
                                                    [relation],
                                                    mode=1)
        if relation != new_fact_name:
            self.replace(relation, new_fact_name)
    
    def lift_all(self):
        for relation in self.get_relations():
            self.lift(relation)
    