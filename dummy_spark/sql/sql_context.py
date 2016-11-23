# -*- coding: utf-8 -*-

__author__ = 'willmcginnis'


class SQLContext(object):
    def __init__(self, parkContext, sqlContext=None):
        pass

    @property
    def _ssql_ctx(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def setConf(self, key, value):
        """
        NotImplemented

        :param key:
        :param value:
        :return:
        """
        raise NotImplementedError

    def getConf(self, key, defaultValue):
        """
        NotImplemented

        :param key:
        :param defaultValue:
        :return:
        """
        raise NotImplementedError

    def udf(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    def range(self, start, end=None, step=1, numPartitions=None):
        """
        NotImplemented

        :param start:
        :param end:
        :param step:
        :param numPartitions:
        :return:
        """
        raise NotImplementedError

    def registerFunction(self, name, f, returnType=None):
        """
        NotImplemented

        :param name:
        :param f:
        :param returnType:
        :return:
        """
        raise NotImplementedError

    def _inferSchemaFromList(self, data):
        """
        NotImplemented

        :param data:
        :return:
        """
        raise NotImplementedError

    def _inferSchema(self, rdd, samplingRatio=None):
        """
        NotImplemented

        :param rdd:
        :param samplingRatio:
        :return:
        """
        raise NotImplementedError

    def inferSchema(self, rdd, samplingRatio=None):
        """
        NotImplemented

        :param rdd:
        :param samplingRatio:
        :return:
        """
        raise NotImplementedError

    def applySchema(self, rdd, schema):
        """
        NotImplemented

        :param rdd:
        :param schema:
        :return:
        """
        raise NotImplementedError

    def _createFromRDD(self, rdd, schema, samplingRatio):
        """
        NotImplemented

        :param rdd:
        :param schema:
        :param samplingRatio:
        :return:
        """
        raise NotImplementedError

    def _createFromLocal(self, data, schema):
        """
        NotImplemented

        :param data:
        :param schema:
        :return:
        """
        raise NotImplementedError

    def createDataFrame(self, data, schema=None, samplingRatio=None):
        """
        NotImplemented

        :param data:
        :param schema:
        :param samplingRatio:
        :return:
        """
        raise NotImplementedError

    def registerDataFrameAsTable(self, df, tableName):
        """
        NotImplemented

        :param df:
        :param tableName:
        :return:
        """
        raise NotImplementedError

    def parquetFile(self, *paths):
        """
        NotImplemented

        :param paths:
        :return:
        """
        raise NotImplementedError

    def jsonFile(self, path, schema=None, samplingRatio=1.0):
        """
        NotImplemented

        :param path:
        :param schema:
        :param samplingRatio:
        :return:
        """
        raise NotImplementedError

    def jsonRDD(self, rdd, schema=None, samplingRatio=1.0):
        """
        NotImplemented

        :param rdd:
        :param schema:
        :param samplingRatio:
        :return:
        """
        raise NotImplementedError

    def load(self, path=None, source=None, schema=None, **options):
        """
        NotImplemented

        :param path:
        :param source:
        :param schema:
        :param options:
        :return:
        """
        raise NotImplementedError

    def createExternalTable(self, tableName, path=None, source=None, schema=None, **options):
        """
        NotImplemented

        :param tableName:
        :param path:
        :param source:
        :param schema:
        :param options:
        :return:
        """
        raise NotImplementedError

    def sql(self, sqlQuery):
        """
        NotImplemented

        :param sqlQuery:
        :return:
        """
        raise NotImplementedError

    def table(self, tableName):
        """
        NotImplemented

        :param tableName:
        :return:
        """
        raise NotImplementedError

    def tables(self, dbName=None):
        """
        NotImplemented

        :param dbName:
        :return:
        """
        raise NotImplementedError

    def tableNames(self, dbName=None):
        """
        NotImplemented

        :param dbName:
        :return:
        """
        raise NotImplementedError

    def cacheTable(self, tableName):
        """
        NotImplemented

        :param tableName:
        :return:
        """
        raise NotImplementedError

    def uncacheTable(self, tableName):
        """
        NotImplemented

        :param tableName:
        :return:
        """
        raise NotImplementedError

    def clearCache(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError

    @property
    def read(self):
        """
        NotImplemented

        :return:
        """
        raise NotImplementedError
