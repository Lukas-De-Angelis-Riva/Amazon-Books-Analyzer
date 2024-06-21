import uuid
import csv
import os

class PersistentList():
    def __init__(self, path):
        self.path = path
        self.list = []
        self.tmp_file =  path + '_tmp'

        if not os.path.exists(self.path):
            open(self.path, 'w').close()

    def append(self, item):
        self.list.append(item)

        with open(self.path, "a+") as tmp_fp:
            bucket = csv.writer(tmp_fp, delimiter=";")
            bucket.writerow([str(item)])
        
    def __getitem__(self, index):
        return self.list[index]

    def __len__(self):
        return self.list.__len__()

    def __iter__(self):
        return self.list.__iter__()

    def __repr__(self):
        return self.list.__repr__()

    def load(self):
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            with open(self.path, "r") as f:
                aux = f.readlines()
            self.worked_chunks = [uuid.UUID(u.strip()) for u in aux]