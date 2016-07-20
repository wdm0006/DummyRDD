DummyRDD
========

[![Coverage Status](https://coveralls.io/repos/github/wdm0006/DummyRDD/badge.svg?branch=master)](https://coveralls.io/github/wdm0006/DummyRDD?branch=master)
[![Build Status](https://travis-ci.org/wdm0006/DummyRDD.svg?branch=master)](https://travis-ci.org/wdm0006/DummyRDD)

Contributors
------------

 * [Henrique Souza](https://github.com/htssouza)
 * [Will McGinnis](https://gitbhub.com/wdm0006)
 
Overview
--------

A test class that walks like and RDD, talks like an RDD but is just a list.

Contains 3 primary classes:

 * SparkConf
 * SparkContext
 * RDD
 
All of which implement the exact same API as the real spark methods, but use a simple
python list as the actual datastore.  Many functions such as the Hadoop API, partitioning, complex
operations, and other things are not implemented.  See below for detailed list of implemented functions and
their caveats. 

Note that for now this is experimental, and may later be useful for testing or development, but anything
developed using this should always be checked on real spark to make sure that things actually work there. Because
none of the code is actually distributed in this environment, some things will behave differently.

It is intended that this library can be used as a drop in replacement for a real spark context, without erroring out
but maybe not actually doing anything (in the case of irrelevant configuration options, for example).

Currently there is no support for the dataframe api, or for that matter most features of anything, very much
still a work in progress.

Example
-------

A quick example:

    from dummy_spark import SparkContext, SparkConf
    
    sconf = SparkConf()
    sc = SparkContext(master='', conf=sconf)
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    
    print(rdd.count())
    print(rdd.map(lambda x: x**2).collect())
   
yields:
    
    5
    [1, 4, 9, 16, 25]


Methods Implemented
===================

SparkConf
---------

SparkConf has everything implemented, but nothing is actually ever set.  There are no real configuration settings for 
the dummy version, so the object simply contains a dictionary of configuration parameters. Implemented functions are therefore:

 * \_\_init\_\_()
 * contains()
 * get()
 * getAll()
 * set()
 * setAll()
 * setAppName()
 * setExecutorEnv()
 * setIfMissing()
 * setMaster()
 * setSparkHome()
 * toDebugString()

SparkContext
------------

Implemented functions are:

 * \_\_init\_\_()
 * \_\_enter\_\_()
 * \_\_exit\_\_()
 * defaultMinPartitions()
 * defaultParallelism()
 * emptyRDD()
 * parallelize()
 * NewAPIHadoopRDD() (only for elasticsearch via elasticsearch-py)
 * range()
 * startTime()
 * stop()
 * textFile() (including from s3 via tinys3)
 * version()

RDD
---

Implemented functions are:

 * \_\_init\_\_()
 * \_\_add\_\_()
 * \_\_repr\_\_()
 * cache()
 * cartesian()
 * checkpoint()
 * cogroup()
 * collect()
 * context()
 * count()
 * countApprox()
 * countApproxDistinct()
 * distinct()
 * filter()
 * first()
 * flatMap()
 * flatMapValues()
 * foreach()
 * foreachPartition()
 * getNumPartitions()
 * glom()
 * groupBy()
 * groupByKey()
 * id()
 * intersection()
 * isEmpty()
 * lookup()
 * map()
 * mapPartitions()
 * mapValues()
 * max()
 * mean()
 * meanApprox()
 * min()
 * name()
 * persist()
 * reduceByKey()
 * repartitionAndSortWithinPartitions()
 * sample()
 * setName()
 * sortBy()
 * sortByKey()
 * sum()
 * take()
 * takeSample()
 * toLocalIterator()
 * union()
 * zip()
 * zipWithIndex()
