import unittest
import phoenixdb
from phoenixdb.tests import TEST_DB_URL

@unittest.skipIf(TEST_DB_URL is None, "these tests require the PHOENIXDB_TEST_DB_URL environment variable set to a clean database")
class PhoenixTypesTest(unittest.TestCase):
    
    def setUp(self):
        self.conn = phoenixdb.connect(TEST_DB_URL, autocommit=True)
        self.cleanup_tables = []

    def tearDown(self):
        self.doCleanups()
        self.conn.close()

    def addTableCleanup(self, name):
        def dropTable():
            with self.conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS {}".format(name))
        self.addCleanup(dropTable)

    def createTable(self, name, columns):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS {}".format(name))
            cursor.execute("CREATE TABLE {} ({})".format(name, columns))
            self.addTableCleanup(name)

    def checkIntType(self, type_name, min_value, max_value):
        self.createTable("phoenixdb_test_tbl1", "id integer primary key, val {}".format(type_name))
        with self.conn.cursor() as cursor:
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (1, 1)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (2, NULL)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (3, ?)", [1])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (4, ?)", [None])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (5, ?)", [min_value])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (6, ?)", [max_value])
            cursor.execute("SELECT id, val FROM phoenixdb_test_tbl1 ORDER BY id")
            self.assertEqual(cursor.description[1].type_code, phoenixdb.NUMBER)
            self.assertEqual(cursor.fetchall(), [[1, 1], [2, None], [3, 1], [4, None], [5, min_value], [6, max_value]])
            self.assertRaises(phoenixdb.DatabaseError, cursor.execute, "UPSERT INTO phoenixdb_test_tbl1 VALUES (100, ?)", [min_value - 1])
            self.assertRaises(phoenixdb.DatabaseError, cursor.execute, "UPSERT INTO phoenixdb_test_tbl1 VALUES (100, ?)", [max_value + 1])

    def test_integer(self):
        self.checkIntType("integer", -2147483648, 2147483647)

    def test_unsigned_int(self):
        self.checkIntType("unsigned_int", 0, 2147483647)

    def test_bigint(self):
        self.checkIntType("bigint", -9223372036854775808, 9223372036854775807)

    def test_unsigned_long(self):
        self.checkIntType("unsigned_long", 0, 9223372036854775807)

    def test_tinyint(self):
        self.checkIntType("tinyint", -128, 127)

    @unittest.skip("https://issues.apache.org/jira/browse/PHOENIX-2082")
    def test_unsigned_tinyint(self):
        self.checkIntType("unsigned_tinyint", 0, 127)

    def test_smallint(self):
        self.checkIntType("smallint", -32768, 32767)

    def test_unsigned_smallint(self):
        self.checkIntType("unsigned_smallint", 0, 32767)

    def checkFloatType(self, type_name, min_value, max_value):
        self.createTable("phoenixdb_test_tbl1", "id integer primary key, val {}".format(type_name))
        with self.conn.cursor() as cursor:
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (1, 1)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (2, NULL)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (3, ?)", [1])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (4, ?)", [None])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (5, ?)", [min_value])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (6, ?)", [max_value])
            cursor.execute("SELECT id, val FROM phoenixdb_test_tbl1 ORDER BY id")
            self.assertEqual(cursor.description[1].type_code, phoenixdb.NUMBER)
            rows = cursor.fetchall()
            self.assertEqual([r[0] for r in rows], [1, 2, 3, 4, 5, 6])
            self.assertEqual(rows[0][1], 1.0)
            self.assertEqual(rows[1][1], None)
            self.assertEqual(rows[2][1], 1.0)
            self.assertEqual(rows[3][1], None)
            self.assertAlmostEqual(rows[4][1], min_value)
            self.assertAlmostEqual(rows[5][1], max_value)

    def test_float(self):
        self.checkFloatType("float", -3.4028234663852886e+38, 3.4028234663852886e+38)

    def test_unsigned_float(self):
        self.checkFloatType("unsigned_float", 0, 3.4028234663852886e+38)

    def test_double(self):
        self.checkFloatType("double", -1.7976931348623158E+308, 1.7976931348623158E+308)

    def test_unsigned_double(self):
        self.checkFloatType("unsigned_double", 0, 1.7976931348623158E+308)

    @unittest.skip("not implemented")
    def test_decimal(self):
        assert False

    def test_boolean(self):
        self.createTable("phoenixdb_test_tbl1", "id integer primary key, val boolean")
        with self.conn.cursor() as cursor:
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (1, TRUE)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (2, FALSE)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (3, NULL)")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (4, ?)", [True])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (5, ?)", [False])
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (6, ?)", [None])
            cursor.execute("SELECT id, val FROM phoenixdb_test_tbl1 ORDER BY id")
            self.assertEqual(cursor.description[1].type_code, phoenixdb.BOOLEAN)
            self.assertEqual(cursor.fetchall(), [[1, True], [2, False], [3, None], [4, True], [5, False], [6, None]])

    @unittest.skip("broken")
    def test_time(self):
        self.createTable("phoenixdb_test_tbl1", "id integer primary key, val time")
        with self.conn.cursor() as cursor:
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (1, '12:00:00')")
            cursor.execute("UPSERT INTO phoenixdb_test_tbl1 VALUES (2, NULL)")
