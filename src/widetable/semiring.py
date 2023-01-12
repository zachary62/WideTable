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
        return CountSemiRing(user_table=user_table, attr=attr)
    
    raise Exception('Unsupported Semiring')

class SemiRing(ABC):
    type: str

    def __init__(self):
        pass

    def lift_exp(self, attr):
        pass
    
    # this is a bit ill defined
    # it is only for semi-ring aggregation over attributes in a single table
    def get_user_table(self):
        pass

    def sum_over_product(self, user_tables=[]):
        pass
    
    def sum_col(self, user_table):
        pass

    def __str__(self):
        pass
    
    def __hash__(self):
        return self.__str__().__hash__ ()

class SemiField(SemiRing):
    def division(self, dividend, divisor):
        pass


class SumSemiRing(SemiRing):
    def __init__(self, user_table="", attr=""):
        self.user_table = user_table
        self.attr = attr

    def lift_exp(self, s_after='s', user_table=""):
        if user_table == self.user_table:
            return {s_after: (self.attr, Aggregator.IDENTITY)}
        else:
            return {s_after: ("1", Aggregator.IDENTITY)}

    def sum_over_product(self, user_tables=[], s='s', s_after='s'):
        sum_join_calculation = {}
        for i, user_table in enumerate(user_tables):
            sum_join_calculation[f'"{user_table}"'] = f'"{s}"'

        return {s_after: (sum_join_calculation, Aggregator.SUM_PROD)}
    
    def sum_col(self, user_table):
        self.sum_over_product([user_table])
    
    def get_user_table(self):
        return self.user_table
    
    def __str__(self, relation=True):
        return f'SUM({(self.user_table + ".") if relation else ""}{self.attr})'


class CountSemiRing(SemiField):

    def __init__(self, user_table="", attr=""):
        self.user_table = user_table
        self.attr = attr

    def lift_exp(self, user_table="", c_after='c'):
        return {c_after: ('1', Aggregator.IDENTITY)}

    def sum_over_product(self, user_tables=[], c='c', c_after='c'):
        annotated_count = {}
        for i, user_table in enumerate(user_tables):
            annotated_count[f'"{user_table}"'] = f'"{c}"'
        return {c_after: (annotated_count, Aggregator.SUM_PROD)}
    
    def sum_col(self, user_table):
        return self.sum_over_product([user_table])
    
    def get_user_table(self):
        return self.user_table
    
    def division(self, dividend, divisor, c='c', c_after='c'):
        return {c_after: ([f'"{dividend}"."{c}"', f'"{divisor}"."{c}"'], Aggregator.DIV)}
    
    def __str__(self, relation=True):
        return f'COUNT({(self.user_table + ".") if relation else ""}{self.attr})'


class AvgSemiRing(SemiField):

    def __init__(self, user_table="", attr=""):
        self.user_table = user_table
        self.attr = attr
        
    def lift_exp(self, s_after='s', c_after='c', user_table=""):
        if user_table == self.user_table:
            return {s_after: (self.attr, Aggregator.IDENTITY), c_after: ("1", Aggregator.IDENTITY)}
        else:
            return {s_after: ("0", Aggregator.IDENTITY), c_after: ("1", Aggregator.IDENTITY)}

    def col_sum(self, s='s', c='c', s_after='s', c_after='c'):
        return {s_after: (s, Aggregator.SUM), c_after: (c, Aggregator.SUM)}

    def sum_over_product(self, user_tables=[], s='s', c='c', s_after='s', c_after='c'):
        annotated_count = {}
        for i, user_table in enumerate(user_tables):
            annotated_count[f'"{user_table}"'] = f'"{c}"'

        sum_join_calculation = []
        for i, user_table in enumerate(user_tables):
            sum_join_calculation.append([f'"{str(user_table)}"."{s}"'] + \
                                        [f'"{rel}"."{c}"' for rel in (user_tables[:i] + user_tables[i+1:])])

        return {s_after: (sum_join_calculation, Aggregator.DISTRIBUTED_SUM_PROD), 
                c_after: (annotated_count, Aggregator.SUM_PROD)}
    
    def sum_col(self, user_table):
        return self.sum_over_product([user_table])
    
    # we assume that divisor.s is 0
    def division(self, dividend, divisor, s='s', c='c', s_after='s', c_after='c'):
        return {s_after: ([f'"{dividend}"."{s}"', f'"{divisor}"."{c}"'], Aggregator.DIV),
                c_after: ([f'"{dividend}"."{c}"', f'"{divisor}"."{c}"'], Aggregator.DIV)}
    
    def get_value(self):
        return self.r_pair
    
    def get_user_table(self):
        return self.user_table
    
    def __str__(self,relation=True):
        return f'AVG({(self.user_table + ".") if relation else ""}{self.attr})'

# check if expression has all identity aggregator
# this is for optimization: if all identity aggregator, we don't need to materialize the result
def all_identity(expression):
    for key in expression:
        _, agg = expression[key]
        if agg != Aggregator.IDENTITY:
            return False
    return True
