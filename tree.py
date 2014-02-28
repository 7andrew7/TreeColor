
import abc
import collections

NUM_ATTRIBUTES = 4

class Node(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_costs(self):
        """Return a dictionary of costs to product output with a given paritioning."""

    @abc.abstractmethod
    def set_color(self, color):
        """Set the partition attribute for the given node and all its descendents."""

class ScanNode(Node):
    """Represents a 'scan' of a base table."""

    def __init__(self, partition_set, num_columns=4, num_tuples=1000):
        self.partition_set = partition_set
        self.num_columns = num_columns
        self.num_tuples = num_tuples
        self.color = None

    def get_costs(self):
        costs = dict([(x, self.num_tuples) for x in range(self.num_columns)])
        costs.update(dict([(x, 0) for x in self.partition_set]))
        costs[-1] = 0

        return costs

    def set_color(self, color):
        assert color in range(self.num_columns)
        self.color = color

if __name__ == '__main__':
    n = ScanNode({1,2})
    print n.get_costs()
