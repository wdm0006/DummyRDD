import uuid
import random
import itertools
from dummy_spark.resultsiterable import ResultIterable

__author__ = 'willmcginnis'


class RDD(object):
    """
    A Resilient Distributed Dataset (RDD) is the basic abstraction in Spark. It represents an immutable, partitioned
    collection of elements that can be operated on in parallel. This is a dummy version of that, that is just a list
    under the hood.  To be used for testing, and maybe development if you play fast and loose.

    Important note: the dummy RDD is NOT lazily loaded.

    """

    def __init__(self, jrdd, ctx, jrdd_deserializer=None):
        # ported
        self._id = str(uuid.uuid4())

        if jrdd is None:
            self._jrdd = []
        else:
            if isinstance(jrdd, list):
                self._jrdd = jrdd
            elif isinstance(jrdd, set):
                self._jrdd = list(jrdd)
            else:
                raise AttributeError('Type %s for jrdd not supported' % (type(jrdd), ))

        self.ctx = ctx
        self.is_cached = True
        self.name = 'dummpy-rdd'

        # not ported

        self.is_checkpointed = False

        self._jrdd_deserializer = jrdd_deserializer
        self.partitioner = None

    def _pickled(self):
        raise NotImplementedError

    def id(self):
        return self._id

    def __repr__(self):
        return str(self._jrdd)

    @property
    def context(self):
        return self.ctx

    def cache(self):
        return self

    def persist(self, storageLevel=None):
        return self

    def unpersist(self):
        return self

    def checkpoint(self):
        pass

    def isCheckpointed(self):
        return True

    def getCheckpointFile(self):
        return None

    def map(self, f, preservesPartitioning=False):
        data = list(map(f, self._jrdd))
        return RDD(data, self.ctx)

    def flatMap(self, f, preservesPartitioning=False):
        data = [item for sl in map(f, self._jrdd) for item in sl]
        return RDD(data, self.ctx)

    def mapPartitions(self, f, preservesPartitioning=False):
        return self.map(f, preservesPartitioning=preservesPartitioning)

    def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
        raise NotImplementedError

    def getNumPartitions(self):
        return 1

    def filter(self, f):
        data = list(filter(f, self._jrdd))
        return RDD(data, self.ctx)

    def distinct(self, numPartitions=None):
        data = list(set(self._jrdd))
        return RDD(data, self.ctx)

    def sample(self, withReplacement, fraction, seed=None):
        assert fraction >= 0.0, "Negative fraction value: %s" % fraction

        if seed is not None:
            random.seed(seed)

        if withReplacement:
            out = [self._jrdd[random.choice(list(range(len(self._jrdd))))] for _ in int(fraction) * len(self._jrdd)]
        else:
            idx_list = list(range(len(self._jrdd)))
            random.shuffle(idx_list)
            out = [self._jrdd[idx] for idx in idx_list[:int(fraction) * len(self._jrdd)]]
        return out

    def randomSplit(self, weights, seed=None):
        pass

    def takeSample(self, withReplacement, num, seed=None):
        assert num >= 0.0, "Negative sample num: %s" % num

        if seed is not None:
            random.seed(seed)

        if withReplacement:
            out = [self._jrdd[random.choice(list(range(len(self._jrdd))))] for _ in num]
        else:
            idx_list = list(range(len(self._jrdd)))
            random.shuffle(idx_list)
            out = [self._jrdd[idx] for idx in idx_list[:num]]
        return out

    @staticmethod
    def _computeFractionForSampleSize(sampleSizeLowerBound, total, withReplacement):
        raise NotImplementedError

    def union(self, other):
        data = self._jrdd
        data += other._jrdd
        new_rdd = RDD(data, self.ctx)
        return new_rdd

    def intersection(self, other):
        return self.map(lambda v: (v, None)) \
            .cogroup(other.map(lambda v: (v, None))) \
            .filter(lambda k_vs: all(k_vs[1])) \
            .keys()

    def _reserialize(self, serializer=None):
        return self

    def __add__(self, other):
        if not isinstance(other, RDD):
            raise TypeError
        return self.union(other)

    def repartitionAndSortWithinPartitions(self, numPartitions=None, partitionFunc=None, ascending=True, keyfunc=lambda x: x):
        data = sorted(self._jrdd, key=keyfunc, reverse=ascending)
        return RDD(data, self.ctx)

    def sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x):
        data = sorted(self._jrdd, key=keyfunc, reverse=ascending)
        return RDD(data, self.ctx)

    def sortBy(self, keyfunc, ascending=True, numPartitions=None):
        data = sorted(self._jrdd, key=keyfunc, reverse=ascending)
        return RDD(data, self.ctx)

    def glom(self):
        return self._jrdd

    def cartesian(self, other):
        data = list(itertools.product(self._jrdd, other))
        return RDD(data, self.ctx)

    def groupBy(self, f, numPartitions=None):
        return self.map(lambda x: (f(x), x)).groupByKey(numPartitions)

    def pipe(self, command, env=None):
        raise NotImplementedError

    def foreach(self, f):
        return self.map(f)

    def foreachPartition(self, f):
        def func(it):
            r = f(it)
            try:
                return iter(r)
            except TypeError:
                return iter([])
        self.mapPartitions(func).count()

    def collect(self):
        return self._jrdd

    def reduce(self, f):
        raise NotImplementedError

    def treeReduce(self, f, depth=2):
        raise NotImplementedError

    def fold(self, zeroValue, op):
        raise NotImplementedError

    def aggregate(self, zeroValue, seqOp, combOp):
        raise NotImplementedError

    def treeAggregate(self, zeroValue, seqOp, combOp, depth=2):
        raise NotImplementedError

    def max(self, key=None):
        if key is None:
            return max(self._jrdd)
        else:
            raise NotImplementedError

    def min(self, key=None):
        if key is None:
            return min(self._jrdd)
        else:
            raise NotImplementedError

    def sum(self):
        return sum(self._jrdd)

    def count(self):
        return len(self._jrdd)

    def stats(self):
        raise NotImplementedError

    def histogram(self, buckets):
        raise NotImplementedError

    def mean(self):
        return float(sum(self._jrdd)) / len(self._jrdd)

    def variance(self):
        raise NotImplementedError

    def stdev(self):
        raise NotImplementedError

    def sampleStdev(self):
        raise NotImplementedError

    def sampleVariance(self):
        raise NotImplementedError

    def countByValue(self):
        raise NotImplementedError

    def top(self, num, key=None):
        return NotImplementedError

    def takeOrdered(self, num, key=None):
        raise NotImplementedError

    def take(self, num):
        return self._jrdd[:num]

    def first(self):
        return self._jrdd[0]

    def isEmpty(self):
        return len(self._jrdd) == 0

    def saveAsNewAPIHadoopDataset(self, conf, keyConverter=None, valueConverter=None):
        raise NotImplementedError

    def saveAsNewAPIHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None):
        raise NotImplementedError

    def saveAsHadoopDataset(self, conf, keyConverter=None, valueConverter=None):
        raise NotImplementedError

    def saveAsHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None, compressionCodecClass=None):
        raise NotImplementedError

    def saveAsSequenceFile(self, path, compressionCodecClass=None):
        raise NotImplementedError

    def saveAsPickleFile(self, path, batchSize=10):
        raise NotImplementedError

    def saveAsTextFile(self, path, compressionCodecClass=None):
        raise NotImplementedError

    # Pair functions

    def collectAsMap(self):
        raise NotImplementedError

    def keys(self):
        raise NotImplementedError

    def values(self):
        raise NotImplementedError

    def reduceByKey(self, func, numPartitions=None):
        raise NotImplementedError

    def reduceByKeyLocally(self, func):
        raise NotImplementedError

    def countByKey(self):
        raise NotImplementedError

    def join(self, other, numPartitions=None):
        raise NotImplementedError

    def leftOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError

    def rightOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError

    def fullOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError

    def partitionBy(self, numPartitions, partitionFunc=None):
        raise NotImplementedError

    def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None):
        raise NotImplementedError

    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None):
        raise NotImplementedError

    def foldByKey(self, zeroValue, func, numPartitions=None):
        raise NotImplementedError

    def _can_spill(self):
        raise NotImplementedError

    def _memory_limit(self):
        raise NotImplementedError

    # TODO: support variant with custom partitioner
    def groupByKey(self, numPartitions=None):
        keys = {x[0] for x in self._jrdd}
        out = {k: ResultIterable([x for x in self._jrdd if x[0] == k]) for k in keys}
        data = list(out.items())
        return RDD(data, self.ctx)

    def flatMapValues(self, f):
        flat_map_fn = lambda kv: ((kv[0], x) for x in f(kv[1]))
        return self.flatMap(flat_map_fn, preservesPartitioning=True)

    def mapValues(self, f):
        map_values_fn = lambda kv: (kv[0], f(kv[1]))
        return self.map(map_values_fn, preservesPartitioning=True)

    def groupWith(self, other, *others):
        raise NotImplementedError

    # TODO: add variant with custom parittioner
    def cogroup(self, other, numPartitions=None):
        raise NotImplementedError

    def sampleByKey(self, withReplacement, fractions, seed=None):
        raise NotImplementedError

    def subtractByKey(self, other, numPartitions=None):
        raise NotImplementedError

    def subtract(self, other, numPartitions=None):
        raise NotImplementedError

    def keyBy(self, f):
        raise NotImplementedError

    def repartition(self, numPartitions):
        raise NotImplementedError

    def coalesce(self, numPartitions, shuffle=False):
        raise NotImplementedError

    def zip(self, other):
        data = list(zip(other, self._jrdd))
        return RDD(data, self.ctx)

    def zipWithIndex(self):
        data = [(b, a) for a, b in list(enumerate(self._jrdd))]
        return RDD(data, self.ctx)

    def zipWithUniqueId(self):
        raise NotImplementedError

    def name(self):
        return self.name

    def setName(self, name):
        self.name = name
        return self

    def toDebugString(self):
        raise NotImplementedError

    def getStorageLevel(self):
        raise NotImplementedError

    def _defaultReducePartitions(self):
        return 1

    def lookup(self, key):
        return [x for x in self._jrdd if x[0] == key]

    def _to_java_object_rdd(self):
        raise NotImplementedError

    def countApprox(self, timeout, confidence=0.95):
        return len(self._jrdd)

    def sumApprox(self, timeout, confidence=0.95):
        return sum(self._jrdd)

    def meanApprox(self, timeout, confidence=0.95):
        return float(sum(self._jrdd)) / len(self._jrdd)

    def countApproxDistinct(self, relativeSD=0.05):
        return len(set(self._jrdd))

    def toLocalIterator(self):
        for row in self._jrdd:
            yield row