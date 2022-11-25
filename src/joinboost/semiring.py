from abc import ABC, abstractmethod
from copy import deepcopy
from .aggregator import Aggregator, Message
from .joingraph import JoinGraph

'''Handle semi ring in DBMS'''


def SemiRingFactory(name, 
                    user_table, 
                    attr):
    name = name.strip().lower()
    # By default if con is not specified, user uses Pandas dataframe
    if name == "sum":
        return SumSemiRing(user_table=user_table, attr=attr)
    if name == "avg":
        return AvgSemiRing(user_table=user_table, attr=attr)
    if name == "mean":
        return AvgSemiRing(user_table=user_table, attr=attr)
    if name == "count":
        return SumSemiRing(user_table=user_table)
    
    raise Exception('Unsupported Semiring')

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

    def compute_col_operations(self, user_tables=[], s_after='s', c_after='c', s='s', c='c'):
        pass

    def __str__(self):
        pass
    
    def __hash__(self):
        return self.__str__().__hash__ ()


class SumSemiRing(SemiRing):
    def __init__(self, user_table="", attr=""):
        self.user_table = user_table
        self.attr = attr

    def lift_exp(self, s_after='s', user_table=""):
        if user_table == self.user_table:
            return {s_after: (self.attr, Aggregator.IDENTITY)}
        else:
            return {s_after: ("1", Aggregator.IDENTITY)}

    def compute_col_operations(self, user_tables=[], s='s', s_after='s'):
        sum_join_calculation = {}
        for i, user_table in enumerate(user_tables):
            sum_join_calculation[f'"{user_table}"'] = f'"{s}"'

        return {s_after: (sum_join_calculation, Aggregator.SUM_PROD)}
    
    def get_user_table(self):
        return self.user_table
    
    def __str__(self):
        return f'SUM({self.user_table}.{self.attr})'


class CountSemiRing(SemiRing):

    def __init__(self, user_table=""):
        self.user_table = user_table

    def lift_exp(self, user_table="", c_after='c'):
        return {c_after: ('1', Aggregator.IDENTITY)}

    def compute_col_operations(self, user_tables=[], c='c', c_after='c'):
        annotated_count = {}
        for i, user_table in enumerate(user_tables):
            annotated_count[f'"{user_table}"'] = f'"{c}"'
        return {c_after: (annotated_count, Aggregator.SUM_PROD)}
    
    def get_user_table(self):
        return self.user_table
    
    def __str__(self):
        return f'COUNT({self.user_table}.1)'


class AvgSemiRing(SemiRing):

    def __init__(self, user_table="", attr=""):
        self.user_table = user_table
        self.attr = attr

    def multiplication(self, semi_ring):
        s, c = semi_ring.get_value()
        self.r_pair = (self.r_pair[0] * c + self.r_pair[1] * s, c * self.r_pair[1])

    def lift_exp(self, s_after='s', c_after='c', user_table=""):
        if user_table == self.user_table:
            return {s_after: (self.attr, Aggregator.IDENTITY), c_after: ("1", Aggregator.IDENTITY)}
        else:
            return {s_after: ("0", Aggregator.IDENTITY), c_after: ("1", Aggregator.IDENTITY)}

    def col_sum(self, s='s', c='c', s_after='s', c_after='c'):
        return {s_after: (s, Aggregator.SUM), c_after: (c, Aggregator.SUM)}

    def compute_col_operations(self, user_tables=[], s='s', c='c', s_after='s', c_after='c'):

        annotated_count = {}
        for i, user_table in enumerate(user_tables):
            annotated_count[f'"{user_table}"'] = f'"{c}"'

        sum_join_calculation = []
        for i, user_table in enumerate(user_tables):
            sum_join_calculation.append([f'"{str(user_table)}"."{s}"'] + \
                                        [f'"{rel}"."{c}"' for rel in (user_tables[:i] + user_tables[i+1:])])

        return {s_after: (sum_join_calculation, Aggregator.DISTRIBUTED_SUM_PROD), 
                c_after: (annotated_count, Aggregator.SUM_PROD)}

    def get_value(self):
        return self.r_pair
    
    def get_user_table(self):
        return self.user_table
    
    def __str__(self):
        return f'AVG({self.user_table}.{self.attr})'

# check if expression has all identity aggregator
def all_identity(expression):
    for key in expression:
        _, agg = expression[key]
        if agg != Aggregator.IDENTITY:
            return False
    return True
