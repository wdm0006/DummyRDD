# -*- coding: utf-8 -*-

import os
import random

from dummy_spark import SparkContext, SparkConf
from dummy_spark.sql import SQLContext

__author__ = 'willmcginnis'

# make a spark conf
sconf = SparkConf()

# set some property (won't do anything)
sconf.set('spark.executor.extraClassPath', 'foo')

# use the spark conf to make a spark context
sc = SparkContext(master='', conf=sconf)

# set the log level (also doesn't do anything)
sc.setLogLevel('INFO')

# maybe make a useless sqlcontext (nothing implimented here yet)
sqlctx = SQLContext(sc)

# add pyfile just appends to the sys path
sc.addPyFile(os.path.dirname(__file__))

# do some hadoop configuration into the ether
sc._jsc.hadoopConfiguration().set('foo', 'bar')

# maybe make some data
rdd = sc.parallelize([1, 2, 3, 4, 5])

# map and collect
print('\nmap()')
rdd = rdd.map(lambda x: x ** 2)
print(rdd.collect())

# add some more in there
print('\nunion()')
rdd2 = sc.parallelize([2, 4, 10])
rdd = rdd.union(rdd2)
print(rdd.collect())

# filter and take
print('\nfilter()')
rdd = rdd.filter(lambda x: x > 4)
print(rdd.take(10))

# flatmap
print('\nflatMap()')
rdd = rdd.flatMap(lambda x: [x, x, x])
print(rdd.collect())

# group by key
print('\ngroupByKey()')
rdd = rdd.map(lambda x: (x, random.random()))
rdd = rdd.groupByKey()
print(rdd.collect())
rdd = rdd.mapValues(list)
print(rdd.collect())

# forEachPartition
print('\nforEachPartition()')
rdd.foreachPartition(lambda x: print('partition: ' + str(x)))

# reducebykey
print('\nReduceByKey')
rdd = sc.parallelize([(0, 'A'), (0, 'A'), (1, 'C')])
rdd = rdd.reduceByKey(lambda a, b: a)
print(rdd)