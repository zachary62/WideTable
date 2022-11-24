from abc import ABC, abstractmethod

class AttributionException(Exception):
    pass

class Attribution(ABC):
    type: str

    def __init__(self):
        pass
    
    
class Single(attribution):
    type: str

    def __init__(self):
        pass
    
class Replicate(attribution):
    type: str

    def __init__(self, fact):
        pass
    
class Average(attribution):
    type: str

    def __init__(self):
        pass