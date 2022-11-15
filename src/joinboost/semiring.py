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

    def compute_col_operations(self, relations=[], s_after='s', c_after='c', s='s', c='c'):
        pass


class SumSemiRing(SemiRing):
    def __init__(self, relation="", attr=""):
        self.relation = relation
        self.attr = attr

    def lift_exp(self, s_after='s', relation=""):
        if relation == self.relation:
            return {s_after: (self.attr, Aggregator.IDENTITY)}
        else:
            return {s_after: ("1", Aggregator.IDENTITY)}

    def compute_col_operations(self, relations=[], s='s', s_after='s'):
        sum_join_calculation = {}
        for i, relation in enumerate(relations):
            sum_join_calculation[f'"{relation}"'] = f'"{s}"'

        return {s_after: (sum_join_calculation, Aggregator.SUM_PROD)}


class CountSemiRing(SemiRing):

    def __init__(self):
        pass

    def lift_exp(self, relation="", c_after='c'):
        return {c_after: ('1', Aggregator.IDENTITY)}

    def compute_col_operations(self, relations=[], c='c', c_after='c'):
        annotated_count = {}
        for i, relation in enumerate(relations):
            annotated_count[f'"{relation}"'] = f'"{c}"'
        return {c_after: (annotated_count, Aggregator.SUM_PROD)}


class AvgSemiRing(SemiRing):

    def __init__(self, relation="", attr=""):
        self.relation = relation
        self.attr = attr

    def multiplication(self, semi_ring):
        s, c = semi_ring.get_value()
        self.r_pair = (self.r_pair[0] * c + self.r_pair[1] * s, c * self.r_pair[1])

    def lift_exp(self, s_after='s', c_after='c', relation=""):
        if relation == self.relation:
            return {s_after: (self.attr, Aggregator.IDENTITY), c_after: ("1", Aggregator.IDENTITY)}
        else:
            return {s_after: ("0", Aggregator.IDENTITY), c_after: ("1", Aggregator.IDENTITY)}

    def col_sum(self, s='s', c='c', s_after='s', c_after='c'):
        return {s_after: (s, Aggregator.SUM), c_after: (c, Aggregator.SUM)}

    def compute_col_operations(self, relations=[], s='s', c='c', s_after='s', c_after='c'):

        annotated_count = {}
        for i, relation in enumerate(relations):
            annotated_count[f'"{relation}"'] = f'"{c}"'

        sum_join_calculation = []
        for i, relation in enumerate(relations):
            sum_join_calculation.append([f'"{str(relation)}"."{s}"'] + [f'"{rel}"."{c}"' for rel in (relations[:i] + relations[i+1:])])

        return {s_after: (sum_join_calculation, Aggregator.DISTRIBUTED_SUM_PROD), c_after: (annotated_count, Aggregator.SUM_PROD)}

    def get_value(self):
        return self.r_pair


# check if expression has all identity aggregator
def all_identity(expression):
    for key in expression:
        _, agg = expression[key]
        if agg != Aggregator.IDENTITY:
            return False
    return True
