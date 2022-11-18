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
        
    def register_measurement(self, semi_ring: SemiRing):
        if semi_ring in self.cjts:
            raise Exception('The measurement has already been registered')
            
        if not self.has_relation(semi_ring.get_relation()):
            raise Exception('The join graph doesn\'t contain the relation for the measurement.')
        
        self.cjts[semi_ring] = CJT(semi_ring=semi_ring, join_graph=self.join_graph)
        self.measurement[semi_ring.get_relation()] = semi_ring
        
    # TODO    
    def _repr_html_(self):
        return "hi"
        