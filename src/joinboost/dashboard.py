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
        return semi_ring

    '''
    node structure:
    nodes: [
        { 
            id: table_name,
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
        for table_name in self.relation_schema:
            dimensions = set(self.get_relation_attributes(table_name))
            join_keys = set(self.get_join_keys(table_name))
            attributes = set(self.get_useful_attributes(table_name))
            measurements = set()
            if table_name in self.measurement:
                measurement = self.measurement[table_name].__str__()
                measurements.add(measurement)
                attributes.add(measurement)
            nodes.append({"id": table_name,
                          "attributes": list(attributes),
                          "join_keys": list(join_keys),
                          "measurements": list(measurements)
                          })

        # avoid edge in opposite direction
        seen = set()
        for table_name_left in self.joins:
            for table_name_right in self.joins[table_name_left]:
                if (table_name_right, table_name_left) in seen:
                    continue
                keys = self.joins[table_name_left][table_name_right]['keys']
                left_mul = self.get_multiplicity(table_name_left, table_name_right)
                right_mul = self.get_multiplicity(table_name_right, table_name_left)
                links.append({"source": table_name_left, "target": table_name_right,
                              "left_keys": keys[0], "right_keys": keys[1],
                              "multiplicity": [left_mul, right_mul]})
                seen.add((table_name_left, table_name_right))

        self.session_id += 1

        s = self.rep_template
        s = s.replace("{{session_id}}", str(self.session_id))
        s = s.replace("{{nodes}}", str(nodes))
        s = s.replace("{{links}}", str(links))
        return s