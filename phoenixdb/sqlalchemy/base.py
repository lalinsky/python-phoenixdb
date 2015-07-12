import urlparse
import urllib
import phoenixdb
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import DDLCompiler
from sqlalchemy.exc import CompileError


class PhoenixDDLCompiler(DDLCompiler):

    def visit_primary_key_constraint(self, constraint):
        if constraint.name is None:
            raise CompileError("can't create primary key without a name")
        return DDLCompiler.visit_primary_key_constraint(self, constraint)


class PhoenixDialect(DefaultDialect):
    name = "phoenix"
    driver = "phoenixdb"

    ddl_compiler = PhoenixDDLCompiler

    @classmethod
    def dbapi(cls):
        return phoenixdb

    def create_connect_args(self, url):
        phoenix_url = urlparse.urlunsplit(urlparse.SplitResult(
            scheme='http',
            netloc='{}:{}'.format(url.host, url.port or 8765),
            path='/',
            query=urllib.urlencode(url.query),
            fragment='',
        ))
        return [phoenix_url], {'autocommit': True}

    def do_rollback(self, dbapi_conection):
        pass

    def do_commit(self, dbapi_conection):
        pass

    def has_table(self, connection, table_name, schema=None):
        if schema is None:
            query = "SELECT 1 FROM system.catalog WHERE table_name = ? LIMIT 1"
            params = [table_name.upper()]
        else:
            query = "SELECT 1 FROM system.catalog WHERE table_name = ? AND schema_name = ? LIMIT 1"
            params = [table_name.upper(), schema_name.upper()]
        return connection.execute(query, params).first() is not None
