import copy
from .semiring import *
from .joingraph import JoinGraph
from .cjt import CJT
from .aggregator import *


class DashBoard(JoinGraph):
    def __init__(self,
                 join_graph: JoinGraph):
        self.join_graph = join_graph
        super().__init__(join_graph.exe,
                         join_graph.joins,
                         join_graph.relation_schema)
        
        self.dimension = self.relation_schema
        self.measurement = dict()
        self.cjts = dict()
        
    def register_semiring(self, semi_ring: SemiRing):
        sem_ring_str = semi_ring.__str__()
        
        # "if semi_ring in self.cjts:" doesn't work
        # I have to pass in the string to avoid duplicates
        # Look dumb and need to check out why.
        if sem_ring_str in self.cjts:
            raise Exception('The measurement has already been registered')
            
        if not self.has_relation(semi_ring.get_relation()):
            raise Exception('The join graph doesn\'t contain the relation for the measurement.')
        
        self.cjts[sem_ring_str] = CJT(semi_ring=semi_ring, join_graph=self.join_graph)
        self.measurement[semi_ring.get_relation()] = semi_ring
        
    def register_measurement(self, name, relation, attr):
        semi_ring = SemiRingFactory(name, relation, attr)
        self.register_semiring(semi_ring)
        return semi_ringy
        
    # TODO    
    def _repr_html_(self):
        return "hi"