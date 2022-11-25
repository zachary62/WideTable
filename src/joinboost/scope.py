from abc import ABC, abstractmethod
from .semiring import *

class ScopeException(Exception):
    pass

class Scope(ABC):
    def change_message_from_multiplicity(self, from_mul, to_mul, m_type):
        return m_type
    
class FullJoin(Scope):
    pass
    
class SingleRelation(Scope):
    def __init__(self, relation):
        self.relation = relation
        
    def change_message_from_multiplicity(self, from_mul, to_mul, m_type):
        return Message.IDENTITY
        
# TODO
class ReplicateFact(Scope):
    def __init__(self, fact):
        pass
# TODO
class AverageAttribution(Scope):
    def __init__(self):
        pass