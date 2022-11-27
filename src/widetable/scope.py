from abc import ABC, abstractmethod
from .semiring import *

class ScopeException(Exception):
    pass

class Scope(ABC):
    def change_message(self, from_table, to_table, m_type, joingraph):
        return m_type
    
    def preprocess(self, joingraph):
        pass
    
    # for visualization, return whether it should be highlighted, and the color
    # if color is None, then use the default color
    def highlightEdge(self, from_table, to_table):
        return True, None
    
    def highlightRelation(self, table):
        return True, None
    
    def normalize(self, relation):
        return None
    
class FullJoin(Scope):
    pass
    
class SingleRelation(Scope):
    def __init__(self, relation):
        self.relation = relation
        
    def change_message(self, from_table, to_table, m_type, joingraph):
        return Message.IDENTITY
    
    # for visualization, return whether it should be highlighted, and the color
    def highlightEdge(self, from_table, to_table):
        return False, None
    
    def highlightRelation(self, relation):
        if relation == self.relation:
            return True, None
        return False, None
    
class ReplicateFact(Scope):
    def __init__(self, relation, fact):
        # the relation
        self.relation = relation
        self.fact = fact
        # stores which subtree is valid for queries
        self.edges = set()
    
    def preprocess(self, joingraph):
        # check if there is a one-to-many path from relation to fact
        def dfs_one_to_many(current_table, parent_table = None):
            if current_table == self.fact:
                return True
            if current_table not in joingraph.joins:
                return False
            for c_neighbor in joingraph.joins[current_table]:
                if c_neighbor != parent_table:
                    mul = joingraph.get_multiplicity(current_table, c_neighbor, simple=True)
                    # if there is a one-to-many 
                    if mul == '1':
                        if c_neighbor == self.fact:
                            self.edges.add((current_table, c_neighbor))
                            return True
                        if dfs_one_to_many(c_neighbor, current_table):
                            self.edges.add((current_table, c_neighbor))
                            return True
            return False
        
        if not dfs_one_to_many(self.relation):    
            raise ValueError(f"there is not a one-to-many path from {self.relation} to {self.fact}")
        
        # add all edges along the many to one path
        def dfs_many_to_one(current_table, parent_table = None):
            for c_neighbor in joingraph.joins[current_table]:
                if c_neighbor != parent_table:
                    mul = joingraph.get_multiplicity(c_neighbor, current_table, simple=True) 
                    if mul == '1':
                        self.edges.add((current_table, c_neighbor))
                        dfs_many_to_one(c_neighbor, current_table)
        dfs_many_to_one(self.relation)
   
    def change_message(self, from_table, to_table, m_type, joingraph):
        # table gets renamed after lift
        from_table = joingraph.get_user_table(from_table)
        to_table = joingraph.get_user_table(to_table)
        
        if (from_table, to_table) in self.edges or (to_table, from_table) in self.edges:
            return m_type
        return Message.IDENTITY
    
    # for visualization, return whether it should be highlighted, and the color
    def highlightEdge(self, from_table, to_table):
        if (from_table, to_table) in self.edges or (to_table, from_table) in self.edges:
            return True, None
        return False, None
    
    def highlightRelation(self, relation):
        for from_table, to_table in self.edges:
            if relation == from_table or relation == to_table:
                return True, None
        return False, None
    
class AverageAttribution(Scope):
    def __init__(self, relation):
        self.relation = relation
        # stores which edges require attribution
        self.edges = set()
        self.highlightcolor = "red"
        
    def preprocess(self, joingraph):
        # check if there is a path from relation that causes duplication
        def dfs(current_table, parent_table = None):
            for c_neighbor in joingraph.joins[current_table]:
                if c_neighbor != parent_table:
                    mul = joingraph.get_multiplicity(c_neighbor, current_table, simple=True) 
                    if mul == 'M':
                        # add the path to edges
                        self.edges.add((current_table, c_neighbor))
                    dfs(c_neighbor, current_table)
        dfs(self.relation)   
        
    # TODO: because of normalization, we can introduce identity message
    # need to be careful about missing join keys
    def change_message(self, from_table, to_table, m_type, joingraph):
        return m_type
    
    # for visualization, return whether it should be highlighted, and the color
    def highlightEdge(self, from_table, to_table):
        if (from_table, to_table) in self.edges or (to_table, from_table) in self.edges:
            return True, self.highlightcolor
        return True, None
    
    def highlightRelation(self, relation):
        for from_table, to_table in self.edges:
            if relation == to_table:
                return True, self.highlightcolor
        return True, None
    
    def normalize(self, relation):
        for from_table, to_table in self.edges:
            if relation == to_table:
                return from_table
        return None
    
#     def change_message(self, from_table, to_table, m_type, joingraph):
#         from_mul = self.get_multiplicity(from_table, to_table)
#         to_mul = self.get_multiplicity(from_table, to_table)
#         if (from_mul, to_mul) in self.edges or (to_mul, from_mul) in self.edges:
#             return m_type
#         return Message.IDENTITY
    