
import abc
import collections
import logging

logging.basicConfig(level=logging.DEBUG)

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
        """Return a dictionary of costs to produce output with a given paritioning."""

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

        logging.info("setting color=%d for %r" % (color, self))

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
        costs = {}
        left_costs = self.left.get_costs()
        right_costs = self.right.get_costs()

        # Map from output color to required input colors from children
        self.inputs = {}

        # Map from output color to whether output must be shuffled.
        self.require_shuffle = {}

        # consider inputs partitioned on each pair of join attributes
        min_cost = 10000000
        min_tuple = None

        # Cost of partitioning on a join column is simply the sum of the cost of partitioning
        # the children on the join column.
        for x,y in self.join_columns:
            y_in = y - self.left.get_num_columns()
            cost = left_costs[x] + right_costs[y_in]
            costs[x] = cost
            costs[y] = cost

            self.inputs[x] = (x,y_in)
            self.inputs[y] = (x,y_in)
            self.require_shuffle[x] = False
            self.require_shuffle[y] = False

            if cost < min_cost:
                min_cost = cost
                min_tuple = (x,y_in)

        # we can produce any output partioning by adding a shuffle to the minimum cost output
        default_cost = min_cost + self.get_output_size()

        for x in range(self.get_num_columns()):
            if not x in costs or costs[x] > default_cost:
                costs[x] = default_cost
                self.inputs[x] = min_tuple
                self.require_shuffle[x] = True

        return costs

    def set_color(self, color):
        assert color in range(self.get_num_columns())

        # If the output color matches a join column, push the corresponding color
        # into the children.
        self.color = color
        self.shuffle_output = self.require_shuffle[color]

        logging.info("setting color=%d for %r" % (color, self))

        child_colors = self.inputs[color]
        self.left.set_color(child_colors[0])
        self.right.set_color(child_colors[1])


    def get_num_columns(self):
        return self.left.get_num_columns() + self.right.get_num_columns()

    def get_output_size(self):
        # XXX totally made up
        return self.left.get_output_size() + self.right.get_output_size()

if __name__ == '__main__':
    s1 = ScanNode(set(), num_columns=2)
    s2 = ScanNode(set(), num_columns=2)
    j1 = JoinNode(s1, s2, {(0,2),(1,3)})

    s3 = ScanNode(set(), num_columns=2)
    j2 = JoinNode(j1, s3, {(0, 4)})

    logging.info(j2.get_costs())
    min_cost = 10000000
    min_color = None

    for color, cost in j2.get_costs().iteritems():
        if cost < min_cost:
            min_cost = cost
            min_color = color

    j2.set_color(min_color)
