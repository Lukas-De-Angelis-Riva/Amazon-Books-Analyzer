import json
import os

class PersistentMap():
    def __init__(self, path):
        self.path = path
        self.map = {}
        self.tmp_file =  path + '_tmp'

        if not os.path.exists(self.path):
            open(self.path, 'w').close()

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

    def __len__(self):
        return self.map.__len__()

    def values(self):
        return self.map.values()

    def keys(self):
        return self.map.keys()

    def items(self):
        return self.map.items()

    def flush(self):
        with open(self.tmp_file, "w") as tmp_fp:
            json.dump({
                k: v.encode() if hasattr(v, 'encode') else v
                for k, v in self.map.items()
            }, tmp_fp)

        os.rename(self.tmp_file, self.path)

    def load(self, parser):
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path, "r") as f:
                aux = json.load(f)
            self.map = {
                k: parser(k, v)
                for k, v in aux.items()
            }
