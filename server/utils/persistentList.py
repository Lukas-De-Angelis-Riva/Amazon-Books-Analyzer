import uuid
import os


class PersistentList():
    def __init__(self, path):
        self.path = path
        self.list = []
        self.tmp_file = path + '_tmp'

        if not os.path.exists(self.path):
            open(self.path, 'w').close()

    def append(self, item):
        self.list.append(item)

        with open(self.path, "a+") as fp:
            fp.write(str(item)+'\n')

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
            with open(self.path, 'r') as fp:
                aux = fp.readlines()
            if len(aux[-1]) != 32+4+1:
                del aux[-1]
                with open(self.tmp_file, 'w') as tmp:
                    tmp.writelines(aux)
                    tmp.flush()
                os.rename(self.tmp_file, self.path)

            self.list = [uuid.UUID(u.strip()) for u in aux]
