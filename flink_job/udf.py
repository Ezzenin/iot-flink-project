"""Custom aggregate function for the median.

Flink 1.20 SQL has no built-in median or PERCENTILE function, so the median of
humidity is computed with a user defined aggregate function. The accumulator
keeps every value seen in the window, then sorts and takes the middle on
finalization. The window is one minute, so the value count stays small and
holding the values in memory is fine.
"""

from pyflink.table import AggregateFunction, DataTypes
from pyflink.table.udf import udaf


class Median(AggregateFunction):

    def create_accumulator(self):
        # A plain list of the humidity values collected in the window.
        return []

    def accumulate(self, accumulator, value):
        if value is not None:
            accumulator.append(float(value))

    def retract(self, accumulator, value):
        if value is not None and float(value) in accumulator:
            accumulator.remove(float(value))

    def merge(self, accumulator, accumulators):
        for other in accumulators:
            accumulator.extend(other)

    def get_value(self, accumulator):
        values = sorted(accumulator)
        n = len(values)
        if n == 0:
            return None
        mid = n // 2
        if n % 2 == 1:
            return float(values[mid])
        return (values[mid - 1] + values[mid]) / 2.0

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.DOUBLE())

    def get_result_type(self):
        return DataTypes.DOUBLE()


median = udaf(
    Median(),
    result_type=DataTypes.DOUBLE(),
    accumulator_type=DataTypes.ARRAY(DataTypes.DOUBLE()),
)
