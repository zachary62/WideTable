import copy
from .semiring import *
from .joingraph import JoinGraph
from .cjt import CJT
from .aggregator import *
import pkgutil

class DashBoard(JoinGraph):
    def __init__(self,
                 join_graph: JoinGraph):
        super().__init__(join_graph.exe,
                         join_graph.joins,
                         join_graph.relation_info)
        
        self.measurement = dict()
        self.cjts = dict()
        
        # template for jupyter notebook display
        self.rep_template = data = pkgutil.get_data(__name__, "static/dashboard.html").decode('utf-8')
        
        
    def register_semiring(self, semi_ring: SemiRing):
        sem_ring_str = semi_ring.__str__()
        
        # "if semi_ring in self.cjts:" doesn't work
        # I have to pass in the string to avoid duplicates
        # Look dumb and need to check out why.
        if sem_ring_str in self.cjts:
            raise Exception('The measurement has already been registered')
            
        if not self.has_relation(semi_ring.get_relation()):
            raise Exception('The join graph doesn\'t contain the relation for the measurement.')
        
        self.cjts[sem_ring_str] = CJT(semi_ring=semi_ring, join_graph=self)
        self.measurement[semi_ring.get_relation()] = semi_ring
        
    def register_measurement(self, name, relation, attr):
        semi_ring = SemiRingFactory(name, relation, attr)
        self.register_semiring(semi_ring)
        return semi_ring

    '''
    node structure:
    nodes: [
        { 
            id: relation,
            dimensions: [dim1, dim2],
            join_keys: [
                {
                    key: col1
                    TODO:
                    multiplicity: many/one
                },
            ],
            measurements: [ AVG, SUM, COUNT]
        }
    ]
    
    edge structure: [
        {
            source: node_id,
            left_keys: [key1, key2, ...],
            dest: node_id,
            right_keys: [key1, key2, ...]
        }
    ]
    
    '''

    def _repr_html_(self):
        nodes = []
        links = []
        for relation in self.get_relations():
            dimensions = set(self.get_relation_attributes(relation))
            join_keys = set(self.get_join_keys(relation))
            attributes = set(self.get_useful_attributes(relation))
            measurements = set()
            if relation in self.measurement:
                measurement = self.measurement[relation].__str__()
                measurements.add(measurement)
                attributes.add(measurement)
            nodes.append({"id": relation,
                          "attributes": list(attributes),
                          "join_keys": list(join_keys),
                          "measurements": list(measurements)
                          })

        # avoid edge in opposite direction
        seen = set()
        for relation_left in self.joins:
            for relation_right in self.joins[relation_left]:
                if (relation_right, relation_left) in seen:
                    continue
                keys = self.joins[relation_left][relation_right]['keys']
                left_mul = self.get_multiplicity(relation_left, relation_right)
                right_mul = self.get_multiplicity(relation_right, relation_left)
                links.append({"source": relation_left, "target": relation_right,
                              "left_keys": keys[0], "right_keys": keys[1],
                              "multiplicity": [left_mul, right_mul]})
                seen.add((relation_left, relation_right))

        self.session_id += 1

        s = self.rep_template
        s = s.replace("{{session_id}}", str(self.session_id))
        s = s.replace("{{nodes}}", str(nodes))
        s = s.replace("{{links}}", str(links))
        return s