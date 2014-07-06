#! /usr/bin/python

"""Linked list Representation and implementation.
"""

# -*- coding: utf-8 -*-

## ------ Imports ------ ##
## ------ Imports ------ ##


__author__ = "Siva Cn (cnsiva.in@gmail.com)"
__website__ = "http://www.cnsiva.com"


class Node:
    """Referential Structure used to create new nodes"""
    def __init__(self, **kwargs):
        """Constructor."""
        self.data = kwargs.get('filaname', '')
        self.next = None
        self.col_name = None
        self.hashcode = None


class Meta:
    """A meta class contains info about the tree at any moment.
    """
    def __init__(self, **kwargs):
        """Constructor."""
        self.start = None
        self.size = 0

        self._id_start, self._id_end = kwargs.get('_id_range', (-1, -1))

    def __repr__(self):
        """."""
        return "SubList({0}, {1})".format(self._id_start, self._id_end)


class LinkedList:
    def __init__(self, **kwargs):
        """Constructor."""
        self._list = Meta(**kwargs)
        self._list.start = None

    def insert(self, data):
        """Insert a new node with in the tree.
        """
        _temp = Node(data)

        if not self._list.start:
            self._list.start = _temp
            return

        walker = self._list.start

        while walker.next:
            walker = walker.next

        self._list.size += 1
        walker.next = _temp

    def show(self):
        """Display the Tree as it traverses LEFT -> ROOT -> RIGHT."""
        walker = self._list.start

        while walker:
            print walker.data
            walker = walker.next


if __name__ == "__main__":
    """This is the first block of Statements to be executed."""
    tree = LinkedList()   # Root Node
    tree.insert(5)   # child
    tree.insert(5)   # child
    tree.insert(15)  # child
    tree.insert(25)  # child
    tree.insert(1)   # child
    tree.insert(2)   # child
    tree.insert(20)  # child
    tree.insert(500) # child
    tree.insert(3)   # child

    tree.show()
