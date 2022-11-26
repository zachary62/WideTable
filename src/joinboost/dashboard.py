import copy
from .joingraph import JoinGraph
from .cjt import CJT
from .aggregator import *
from .scope import *
import pkgutil

class DashBoardCJT(CJT):
    def __init__(self,
                 semi_ring: SemiRing,
                 join_graph: JoinGraph,
                 scope: Scope):
        
        super().__init__(semi_ring,join_graph)
        self.scope = scope
    
    def prepare_message(self, from_table: str, to_table: str, m_type: Message):
        super().prepare_message(from_table, to_table, m_type)
        m_type = self.scope.change_message(from_table, to_table, m_type, self)
        return m_type

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
        
        
    def register_semiring(self, semi_ring: SemiRing, lazy, scope, replace):
        sem_ring_str = semi_ring.__str__()
        
        # "if semi_ring in self.cjts:" doesn't work
        # I have to pass in the string to avoid duplicates
        # Look dumb and need to check out why.
        if sem_ring_str in self.cjts and not replace:
            raise Exception('The measurement has already been registered')
            
        # TODO: move this to semi-ring preprocess
        if not self.has_relation(semi_ring.get_user_table()):
            raise Exception(f'The join graph doesn\'t contain the {semi_ring.get_user_table()} for the measurement.')
        
        cjt = DashBoardCJT(semi_ring=semi_ring, join_graph=self, scope=scope)
        self.cjts[sem_ring_str] = cjt
        
        if not lazy:
            cjt.lift_all()
            cjt.calibration()
        
        if semi_ring.get_user_table() not in self.measurement:
            self.measurement[semi_ring.get_user_table()] = [semi_ring]
        else:
            self.measurement[semi_ring.get_user_table()].append(semi_ring)
        
    def register_measurement(self, name, relation, attr, lazy=False, scope=FullJoin(), replace=False):
        measurement = SemiRingFactory(name, relation, attr)
        scope.preprocess(self)
        self.register_semiring(measurement, lazy, scope, replace)
        return measurement
    
    # TODO: current group-by is only on the root table
    def absorption(self, measurement, group_by=[], order_by = [], mode=4):
        sem_ring_str = measurement.__str__()
        cjt = self.cjts[sem_ring_str]
        user_table = measurement.user_table
        return cjt.absorption(user_table=user_table, group_by=group_by, order_by=order_by, mode=mode)
    
    def highlightRelation(self, measurement, relation):
        # find the scope of this measurement
        sem_ring_str = measurement.__str__()
        scope = self.cjts[sem_ring_str].scope
        return scope.highlightRelation(relation)
        
    def highlightEdge(self, measurement, from_table, to_tabl):
        # find the scope of this measurement
        sem_ring_str = measurement.__str__()
        scope = self.cjts[sem_ring_str].scope
        return scope.highlightEdge(from_table, to_tabl)
    
    '''
    node structure:
    nodes: [
        { 
            id: relation,
            dimensions: [dim1, dim2],
            join_keys: [
                {
                    key: col1
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
            right_keys: [key1, key2, ...],
        }
    ]
    
    Edge and relation also stores:
            highlight: true/false (control the opacity)
            color: black by default
    These two can be updated in js function through interaction
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
                for semi_ring in self.measurement[relation]:
                    measurement = semi_ring.__str__()
                    measurements.add(measurement)
            nodes.append({"id": relation,
                          "measurements": list(measurements),
                          "attributes": list(measurements) + list(attributes),
                          "join_keys": list(join_keys),
                          })

        # avoid edge in opposite direction
        seen = set()
        for relation_left in self.joins:
            for relation_right in self.joins[relation_left]:
                if (relation_right, relation_left) in seen:
                    continue
                keys = self.joins[relation_left][relation_right]['keys']
                left_mul = self.get_multiplicity(relation_left, relation_right,simple=True)
                right_mul = self.get_multiplicity(relation_right, relation_left,simple=True)
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