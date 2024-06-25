import os

from utils.model.virus import virus


class PersistentMap2():
    def __init__(self, path):
        self.path = path
        self.map = {}
        self.tmp_file = path + '_tmp'

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

    def encode(self):
        b = b''
        for v in self.map.values():
            b += v.encode()
        return b

    def decode(self, fp, decoder):
        end = fp.seek(0, os.SEEK_END)
        fp.seek(0, os.SEEK_SET)
        new_map = {}
        while fp.tell() < end:
            thing = decoder(fp)
            new_map[thing.key()] = thing
        return new_map

    def flush(self):
        with open(self.tmp_file, "wb") as tmp_fp:
            virus.write_corrupt(self.encode(), tmp_fp)

        virus.infect()
        os.rename(self.tmp_file, self.path)
        virus.infect()

    def load(self, decoder):
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path, "rb") as fp:
                self.map = self.decode(fp, decoder)
