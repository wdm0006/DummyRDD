# -*- coding: utf-8 -*-

__author__ = 'willmcginnis'


class SQLContext(object):
    def __init__(self, parkContext, sqlContext=None):
        pass

    @property
    def _ssql_ctx(self):
        raise NotImplementedError

    def setConf(self, key, value):
        raise NotImplementedError

    def getConf(self, key, defaultValue):
        raise NotImplementedError

    def udf(self):
        raise NotImplementedError

    def range(self, start, end=None, step=1, numPartitions=None):
        raise NotImplementedError

    def registerFunction(self, name, f, returnType=None):
        raise NotImplementedError

    def _inferSchemaFromList(self, data):
        raise NotImplementedError

    def _inferSchema(self, rdd, samplingRatio=None):
        raise NotImplementedError

    def inferSchema(self, rdd, samplingRatio=None):
        raise NotImplementedError

    def applySchema(self, rdd, schema):
        raise NotImplementedError

    def _createFromRDD(self, rdd, schema, samplingRatio):
        raise NotImplementedError

    def _createFromLocal(self, data, schema):
        raise NotImplementedError

    def createDataFrame(self, data, schema=None, samplingRatio=None):
        raise NotImplementedError

    def registerDataFrameAsTable(self, df, tableName):
        raise NotImplementedError

    def parquetFile(self, *paths):
        raise NotImplementedError

    def jsonFile(self, path, schema=None, samplingRatio=1.0):
        raise NotImplementedError

    def jsonRDD(self, rdd, schema=None, samplingRatio=1.0):
        raise NotImplementedError

    def load(self, path=None, source=None, schema=None, **options):
        raise NotImplementedError

    def createExternalTable(self, tableName, path=None, source=None, schema=None, **options):
        raise NotImplementedError

    def sql(self, sqlQuery):
        raise NotImplementedError

    def table(self, tableName):
        raise NotImplementedError

    def tables(self, dbName=None):
        raise NotImplementedError

    def tableNames(self, dbName=None):
        raise NotImplementedError

    def cacheTable(self, tableName):
        raise NotImplementedError

    def uncacheTable(self, tableName):
        raise NotImplementedError

    def clearCache(self):
        raise NotImplementedError

    @property
    def read(self):
        raise NotImplementedError
