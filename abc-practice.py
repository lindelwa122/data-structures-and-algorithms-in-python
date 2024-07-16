from abc import ABCMeta, abstractmethod

class Sequence(classmethod=ABCMeta):
    @abstractmethod
    def __len__(self):

    @abstractmethod
    def __getitem__(self, j):

    def __contains__(self, val):
        for i in range(len(self)):
            if self[i] == val:
                return True
        return False

    def index(self, val):
        for i in range(len(self)):
            if self[i] == val:
                return i
        raise ValueError('value not in sequence')

    def count(self, val):
        k = 0
        for i in range(len(self)):
            if self[i] == val:
                k += 1
        return k