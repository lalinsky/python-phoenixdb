import os
import unittest
import phoenixdb
import dbapi20

TEST_URL = os.environ.get('PHOENIXDB_TEST_URL')


@unittest.skipIf(TEST_URL is None, "these tests require the PHOENIXDB_TEST_URL environment variable set to a clean database")
class PhoenixDatabaseAPI20Test(dbapi20.DatabaseAPI20Test):
    driver = phoenixdb
    connect_args = (TEST_URL, )

    ddl1 = 'create table %sbooze (name varchar(20) primary key)' % dbapi20.DatabaseAPI20Test.table_prefix
    ddl2 = 'create table %sbarflys (name varchar(20) primary key, drink varchar(30))' % dbapi20.DatabaseAPI20Test.table_prefix
    insert = 'upsert'

    def test_nextset(self): pass
    def test_setoutputsize(self): pass

    def _connect(self):
        con = dbapi20.DatabaseAPI20Test._connect(self)
        con.autocommit = True
        return con

    def test_None(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL2(cur)
            cur.execute("%s into %sbarflys values ('a', NULL)" % (self.insert, self.table_prefix))
            cur.execute('select drink from %sbarflys' % self.table_prefix)
            r = cur.fetchall()
            self.assertEqual(len(r),1)
            self.assertEqual(len(r[0]),1)
            self.assertEqual(r[0][0],None,'NULL value not returned as None')
        finally:
            con.close()

    def test_autocommit(self):
        con = dbapi20.DatabaseAPI20Test._connect(self)
        self.assertFalse(con.autocommit)
        con.autocommit = True
        self.assertTrue(con.autocommit)
        con.autocommit = False
        self.assertFalse(con.autocommit)
        con.close()

    def test_readonly(self):
        con = dbapi20.DatabaseAPI20Test._connect(self)
        self.assertFalse(con.readonly)
        con.readonly = True
        self.assertTrue(con.readonly)
        con.readonly = False
        self.assertFalse(con.readonly)
        con.close()
