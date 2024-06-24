import random


class Disease(Exception):
    pass


class Virus():
    def __init__(self):
        # benign at first
        self.p = 0
        random.seed(75_74)
        self.disease_counter = 0

    def mutate(self, p):
        self.p = p

    def infect(self):
        if self.p > 0 and random.random() < self.p:
            self.disease_counter += 1
            raise Disease

    def write_corrupt(self, s, fp):
        if self.p > 0 and random.random() < self.p:
            r = int(random.random() * len(s))
            fp.write(s[:r])
            fp.flush()
            self.disease_counter += 1
            raise Disease
        fp.write(s)


virus = Virus()
