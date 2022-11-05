from abc import ABC, abstractmethod
from copy import deepcopy
from .aggregator import Aggregator, Message
from .joingraph import JoinGraph

'''Handle semi ring in DBMS'''
class SemiRing(ABC):
    type: str

    def __init__(self):
        pass
        
    def __add__(self, other):
        pass
    
    def __sub__(self, other):
        pass

    def multiplication(self, semi_ring):
        pass

    def lift_exp(self, attr):
        pass

    def lift_addition(self, attr, constant_):
        pass

    def get_sr_in_select(self, m_type: Message, f_table: str, in_msgs: list, f_table_attrs: list):
        pass

class SumSemiRing(SemiRing):
    def __init__(self, relation, attr):
        pass

class CountSemiRing(SemiRing):
    pass
    
class varSemiRing(SemiRing):
    def __init__(self, s=0, c=0):
        self.r_pair = (s, c)

    def set_semi_ring(self, TS: float, TC: int):
        self.r_pair = (TS, TC)
        
    def __add__(self, other):
        return varSemiRing(self.r_pair[0] + other.r_pair[0], self.r_pair[1] + other.r_pair[1])
        
    def __sub__(self, other):
        return varSemiRing(self.r_pair[0] - other.r_pair[0], self.r_pair[1] - other.r_pair[1])

    def multiplication(self, semi_ring):
        s, c = semi_ring.get_value()
        self.r_pair = (self.r_pair[0] * c + self.r_pair[1] * s, c * self.r_pair[1])

    def lift_exp(self, s='s', c='1', s_after='s', c_after='c'):
        return {s_after: (s, Aggregator.IDENTITY), c_after: (c, Aggregator.IDENTITY)}

    def col_sum(self, s='s', c='c', s_after='s', c_after='c'):
        return {s_after: (s, Aggregator.SUM), c_after: (c, Aggregator.SUM)}

    def col_product_sum(self, relations=[], s='s', c='c', s_after='s', c_after='c'):

        annotated_count = {}
        for i, relation in enumerate(relations):
            annotated_count[f'"{relation}"'] = f'"{c}"'

        sum_join_calculation = {}
        for i, relation in enumerate(relations):
            sum_join_calculation[f'"{str(relation)}"."{s}"'] = [f'"{rel}"."{c}"' for rel in (relations[:i] + relations[i+1:])]

        return {s_after: (sum_join_calculation, Aggregator.DISTRIBUTED_SUM_PROD), c_after: (annotated_count, Aggregator.SUM_PROD)}

    def get_value(self):
        return self.r_pair
    
