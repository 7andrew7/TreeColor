
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

    @abc.abstractmethod
    def get_num_columns(self):
        """Return number of columns."""

    @abc.abstractmethod
    def get_output_size(self, color):
        """Estimate output size for the operator."""

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

    def get_num_columns(self):
        return self.num_columns

    def get_output_size(self):
        return self.num_tuples

class JoinNode(Node):
    """Represent a join of two relations."""

    def __init__(self, left, right, join_columns):
        """
        :type left: Node
        :type right: Node
        :type join_columns: set of join column two-tuples in sorted order
        """
        self.left = left
        self.right = right
        self.join_columns = join_columns

    def get_costs(self):
        pass

    def get_num_columns(self):
        return left.get_num_columns() + right.get_num_columns()

    def get_output_size(self):
        # XXX totally made up
        return left.get_output_size() + right.get_output_size()

if __name__ == '__main__':
    n = ScanNode({1,2})
    print n.get_costs()
