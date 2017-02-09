import luigi
import cx_Oracle


class OracleBaseSQLTask(luigi.Task):
    
    _db = luigi.DictParameter()
    
    def get_conn(self):
        conn_dsn = cx_Oracle.makedsn(
            self._db['host'],
            self._db['port'],
            self._db['name']
        )
        
        return cx_Oracle.connect(
            self._db['user'],
            self._db['pass'],
            conn_dsn
        )
    
    def run(self):
        raise NotImplementedError()
    
    def complete(self):
        raise NotImplementedError()


class OracleSQLStatementTask(OracleBaseSQLTask):
    
    _sql = luigi.Parameter(default='')
    
    def run(self):
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(self._sql)
        conn.commit()
        conn.close()


class OracleSQLFileTask(OracleBaseSQLTask):
    
    _sql_file_path = luigi.Parameter(default='')
    
    def run(self):
        with open(self._sql_file_path, 'r') as sql_file:
            file_sql = sql_file.read()
        sql_statements = file_sql.split(';')
        
        conn = self.get_conn()
        cur = conn.cursor()
        for sql_statement in sql_statements:    
            cur.execute(sql_statement)
        conn.commit()
        conn.close()


