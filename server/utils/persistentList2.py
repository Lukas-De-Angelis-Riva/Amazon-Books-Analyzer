import uuid
import ast
import os

from utils.model.virus import virus

class PersistentList2():
    def __init__(self, path):
        self.path = path
        self.list = []
        self.tmp_file = path + '_tmp'

        if not os.path.exists(self.path):
            open(self.path, 'w').close()

    def append(self, item):
        self.list.append(item)

        with open(self.path, "a+") as fp:
            # fp.write(str(item))
            virus.write_corrupt(f"('{str(item[0])}',{item[1]})\n", fp)

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

            try:
                ast.literal_eval(aux[-1]) 
                if "\n" not in aux[-1]:
                    raise SyntaxError

            except SyntaxError:
                del aux[-1]
                with open(self.tmp_file, 'w') as tmp:
                    tmp.writelines(aux)
                    tmp.flush()
                virus.infect()
                os.rename(self.tmp_file, self.path)
                virus.infect()

            aux_list = [ ast.literal_eval(tuple) for tuple in aux]
            self.list = [(uuid.UUID(u.strip()),v) for u,v in aux_list]