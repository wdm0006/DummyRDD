# -*- coding: utf-8 -*-

import unittest


from dummy_spark.sql import SQLContext


class SQLContextTests (unittest.TestCase):

    def test_init(self):
        ctx = SQLContext(None)
        self.assertIsNotNone(ctx)

    def test_not_implemented_methods(self):
        ctx = SQLContext(None)

        with self.assertRaises(NotImplementedError):
            ctx._ssql_ctx()

        with self.assertRaises(NotImplementedError):
            ctx.setConf(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.getConf(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.udf()

        with self.assertRaises(NotImplementedError):
            ctx.range(None, None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.registerFunction(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx._inferSchemaFromList(None)

        with self.assertRaises(NotImplementedError):
            ctx._inferSchema(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.inferSchema(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.applySchema(None, None)

        with self.assertRaises(NotImplementedError):
            ctx._createFromRDD(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx._createFromLocal(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.createDataFrame(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.registerDataFrameAsTable(None, None)

        with self.assertRaises(NotImplementedError):
            ctx.parquetFile(None)

        with self.assertRaises(NotImplementedError):
            ctx.jsonFile(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.jsonRDD(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.load(None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.createExternalTable(None, None, None, None)

        with self.assertRaises(NotImplementedError):
            ctx.sql(None)

        with self.assertRaises(NotImplementedError):
            ctx.table(None)

        with self.assertRaises(NotImplementedError):
            ctx.tables(None)

        with self.assertRaises(NotImplementedError):
            ctx.tableNames(None)

        with self.assertRaises(NotImplementedError):
            ctx.cacheTable(None)

        with self.assertRaises(NotImplementedError):
            ctx.uncacheTable(None)

        with self.assertRaises(NotImplementedError):
            ctx.clearCache()

        with self.assertRaises(NotImplementedError):
            ctx.read()
