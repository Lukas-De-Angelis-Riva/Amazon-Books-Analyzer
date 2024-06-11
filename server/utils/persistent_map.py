import json
import os


class PersistentMap():
    def __init__(self, path):
        self.path = path
        self.map = {}

        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                self.map = json.load(f)

    def __setitem__(self, k, v):
        self.map.__setitem__(k, v)

    def __delitem__(self, k):
        return self.map.__delitem__(k)

    def __getitem__(self, k):
        return self.map.__getitem__(k)

    def __contains__(self, k):
        return self.map.__contains__(k)
    
    def __iter__(self):
        return self.map.__iter__()

    def __repr__(self):
        return self.map.__repr__()

    def values(self):
        return self.map.values()

    def keys(self):
        return self.map.keys()

    def items(self):
        return self.map.items()

    def flush(self):
        with open(self.path, "w") as f:
            json.dump(self.map, f)
    
