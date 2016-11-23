.. pyspark documentation master file, created by
   sphinx-quickstart on Thu Aug 28 15:17:47 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Dummy Python API Docs!
=================================

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

.. code-block:: python

    from dummy_spark import SparkContext, SparkConf

    sconf = SparkConf()
    sc = SparkContext(master='', conf=sconf)
    rdd = sc.parallelize([1, 2, 3, 4, 5])

    print(rdd.count())
    print(rdd.map(lambda x: x**2).collect())

yields:

.. code-block:: python

    5
    [1, 4, 9, 16, 25]


Contents:

.. toctree::
   :maxdepth: 2

   dummy_spark
   dummy_spark.sql


Core classes:
-------------

    :class:`dummy_spark.SparkContext`

    Main entry point for Spark functionality.

    :class:`dummy_spark.RDD`

    A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.


Indices and tables
==================

* :ref:`search`

