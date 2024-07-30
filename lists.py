class NodeList:
    class Node:
        def __init__(self, value, pointer=None):
            self._value = value
            self._next = pointer

        def get_value(self):
            return self._value
        
        def get_next(self):
            return self._next

        def set_next(self, node):
            self._next = node

    def __init__(self, *args):
        if args:
            self._root = self._create_nodelist(*args)
        else:
            self._root = None

    def _get_nodes(self):
        nodes, cur_node = NodeList(), self._root

        while True:
            if cur_node is None:
                return nodes
            else:
                nodes.append(cur_node)
                cur_node = cur_node.get_next()

    def _create_nodelist(self, *args):
        nodes = NodeList()
        for arg in args:
            node = self.Node(arg)
            nodes.append(node)

        for i, n in enumerate(nodes):
            if i != len(nodes) - 1:
                next_node = nodes[i+1]
                n.set_next(next_node)

        return nodes[0]

    def __repr__(self):
        nodes = self._get_nodes()
        return '[' + ', '.join([f'{n.get_value()}' for n in nodes]) + ']'

    def __len__(self):
        count, node = 0, self._root
        while True:
            if node is None:
                return count
            else:
                count += 1
            node = node.get_next()

    def __getitem__(self, index):
        if not isinstance(index, (int, slice)):
            raise TypeError('list indices must be integers or slices, not str')

        if isinstance(index, slice):
            return self._slice_nodelist(index.start, index.stop, index.step)

        if index < 0: index += len(self)

        if index < 0 or index >= len(self):
            raise IndexError('list index out of range')

        k, node = 0, self._root
        while True:
            if k == index:
                return node.get_value()
            else:
                k += 1
                node = node.get_next()

    def __setitem__(self, index, value):
        if not isinstance(index, int):
            raise TypeError(f'TypeError: list indices must be integers, not {type(index)}')

        nodes = self._get_nodes()
        if index >= len(self) or index < len(self) * -1:
            raise IndexError('list assignment index out of range')
        else:
            new_node = self.Node(value)
            if index in [0, len(self) * -1]:
                next_node = nodes[1]
                new_node.set_next(next_node)
                self._root = new_node
            elif index in [len(self) - 1, -1]:
                nodes[-2].set_next(new_node)
            else:
                prev_node, next_node = nodes[index-1], nodes[index+1]
                prev_node.set_next(new_node)
                new_node.set_next(next_node)


    def __delitem__(self, index):
        if not isinstance(index, (int, slice)):
            raise TypeError('list indices must be integers or slices, not str')

        if isinstance(index, slice):
            def set_val(val, default):
                if val is None: val = default
                return val
            
            start = set_val(index.start, 0)
            stop = set_val(index.stop, len(self))
            step = set_val(index.step, 1)

            if start < 0: start += len(self)
            if stop < 0: stop += len(self)

            indices = range(start, stop, step)
            for i, index in enumerate(indices):
                try: self.pop(index-i)
                except IndexError: continue
        else: 
            self.pop(index)

    def __add__(self, nodelist):
        if not isinstance(nodelist, NodeList):
            raise TypeError(f'can only concatenate NodeList (not "{type(nodelist)}") to NodeList')

        new_nodelist = NodeList()
        for i in self: new_nodelist.append(i)
        for i in nodelist: new_nodelist.append(i)

        return new_nodelist

    def _slice_nodelist(self, start, stop, step):
        def set_val(val, default):
            if val is None: val = default
            return val

        step = set_val(step, 1)
        if step < 0 and start is None:
            start = -1
        if step < 0 and stop is None:
            stop = (len(self)+1) * -1
        
        start, stop = set_val(start, 0), set_val(stop, len(self))

        indices, sliced_list = range(start, stop, step), NodeList()

        for index in indices:
            sliced_list.append(self[index])

        return sliced_list

    def append(self, value):
        new_node, nodes = self.Node(value), self._get_nodes()
        if nodes:
            nodes[-1].set_next(new_node)
        else:
            self._root = new_node

    def extend(self, iterable):
        iterable = iter(iterable)

        for val in iterable:
            self.append(val)

    def insert(self, i, x):
        if not isinstance(i, int):
            raise TypeError(f"'{type(i)}' object cannot be interpreted as an integer")

        if i > len(self):
            i = len(self)
        elif i <= len(self) * -1:
            i = 0

        nodes = self._get_nodes()

        prev_node = None
        if i != 0:
            prev_node = nodes[i-1]

        next_node = None
        if i < len(self):
            next_node = nodes[i]

        new_node = self.Node(x, next_node)
        if prev_node is not None: 
            prev_node.set_next(new_node)
        else:
            self._root = new_node
        
    def remove(self, x):
        nodes = self._get_nodes()

        def find_index(x):
            for i, node in enumerate(nodes):
                if node.get_value() == x:
                    return i
            raise ValueError(f'{x} is not in list')

        iir = find_index(x) # iir -> index of item to remove

        if iir == 0:
            self._root = nodes[1]
        elif iir == len(self) - 1:
            nodes[-2].set_next(None)
        else:
            # Get the node before the one to be removed 
            # and make it point to the one following the one to be remove, 
            # effectively removing the intended node by skipping it
            nodes[iir-1].set_next(nodes[iir+1]) 

    def pop(self, i=-1):
        nodes = self._get_nodes()
        if len(nodes) == 0:
            raise IndexError('pop from empty list')

        elif not isinstance(i, int):
            raise TypeError(f"'{type(i)}' object cannot be interpreted as an integer")

        elif i >= len(self) or i < len(self) * -1:
            raise IndexError('pop index out of range')
        
        elif len(nodes) == 1:
            self._root = None
            return nodes[0].get_value()  
        
        elif i == 0 or i == len(self) * -1:
            self._root = nodes[1]
            return nodes[0].get_value()

        elif i == len(self) - 1 or i == -1:
            nodes[-2].set_next(None)
            return nodes[-1].get_value()

        else:
            nodes[i-1].set_next(nodes[i+1])
            return nodes[i].get_value()

    def clear(self):
        self._root = None

    def index(self, x, start=0, end=None):
        nodes = self._get_nodes()
        
        if end is None: end = len(self)

        if not (isinstance(start, int) and isinstance(end, int)):
            raise TypeError('slice indices must be integers or have an __index__ method')

        for i, n in enumerate(nodes):
            if i >= start and i < end and n.get_value() == x:
                return i
            
        raise ValueError(f'{x} not found in list')

    def count(self, x):
        nodes = self._get_nodes()
        occurrences = 0

        for node in nodes:
            if node.get_value() == x:
                occurrences += 1

        return occurrences

    def copy(self):
        return NodeList(*self)

    def __nextiterator__(self, iterator):
        iterator = list(iterator)
        for i, v in enumerate(iterator):
            if i < len(iterator)-1: yield i, v, iterator[i+1]
            else: yield i, v, None

    def reverse(self):
        nodes = self._get_nodes()

        for i, node, next_node in self.__nextiterator__(reversed(nodes)):
            if i == 0:
                self._root = node
            node.set_next(next_node)

    def sort(self, key=None, reverse=False):
        if not callable(key):
            def key(val): return val

        def merge_sort(ls):
            if len(ls) in [0, 1]:
                return ls
            
            middle = len(ls) // 2
            left = merge_sort(ls[:middle])
            right = merge_sort(ls[middle:])

            sorted_ls = NodeList()
            while len(sorted_ls) != len(ls):
                try:
                    if key(left[0].get_value()) <= key(right[0].get_value()):
                        sorted_ls.append(left.pop(0))
                    else:
                        sorted_ls.append(right.pop(0))
                except IndexError:
                    if left: sorted_ls.append(left.pop(0))
                    else: sorted_ls.append(right.pop(0))

            return sorted_ls

        nodes = self._get_nodes()
        sorted_nodes = merge_sort(nodes)

        def connect_nodes(index, node, next_node):
            if index == 0:
                self._root = node
            node.set_next(next_node)

        if reverse:
            for i, node, next_node in self.__nextiterator__(reversed(sorted_nodes)):
                connect_nodes(i, node, next_node)
        else:
            for i, node, next_node in self.__nextiterator__(sorted_nodes):
                connect_nodes(i, node, next_node)
