
import abc
import collections
import logging

NUM_ATTRIBUTES = 4

class Node(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.color = None
        self.shuffle_output = False

    def get_color(self):
        return self.color

    def is_output_shuffled(self):
        return self.shuffle_output

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

    def __repr__(self):
        return '%r(%r of %d, shuffle=%r)' % (self.__class__, self.get_color(), self.get_num_columns(),
         self.shuffle_output)

class ScanNode(Node):
    """Represents a 'scan' of a base table."""

    def __init__(self, partition_set, num_columns=4, num_tuples=1000):
        Node.__init__(self)

        self.partition_set = partition_set
        self.num_columns = num_columns
        self.num_tuples = num_tuples

    def get_costs(self):
        costs = dict([(x, self.num_tuples) for x in range(self.num_columns)])
        costs.update(dict([(x, 0) for x in self.partition_set]))
        costs[-1] = 0

        return costs

    def set_color(self, color):
        assert color in range(self.num_columns)
        self.color = color

        if color not in self.partition_set:
            self.shuffle_output = True

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
        Node.__init__(self)

        self.left = left
        self.right = right
        self.join_columns = join_columns

        for x,y in self.join_columns:
            assert x < y
            assert y >= self.left.get_num_columns()
            assert x < self.right.get_num_columns()

    def get_costs(self):
        self.costs = {}
        left_costs = self.left.get_costs()
        right_costs = self.right.get_costs()

        # consider inputs partitioned on each pair of join attributes

        for x,y in self.join_columns:
            y_local = y - self.left.get_num_columns()
            cost = left_costs[x] + right_costs[y_local]
            self.costs[x] = cost
            self.costs[y] = cost

        # we can produce any other output partioning by adding a shuffle
        min_cost = min(costs.itervalues())
        shuffle_cost = self.get_output_size()
        default_cost = min_cost + shuffle_cost

        for x in range(self.get_num_columns()):
            if not x in costs or costs[x] > default_cost:
                costs[x] = default_cost

        return costs

    def set_color(self, color):
        assert color in range(self.num_columns)
        self.color = color

        # color the children based 
    def get_num_columns(self):
        return left.get_num_columns() + right.get_num_columns()

    def get_output_size(self):
        # XXX totally made up
        return left.get_output_size() + right.get_output_size()

if __name__ == '__main__':
    n = ScanNode({1,2})
    print n.get_costs()
    print n
