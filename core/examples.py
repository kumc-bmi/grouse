import luigi
from tasks import OracleSQLFileTask


class OracleExampleTask1(OracleSQLFileTask):
    
    _sql_file_path = luigi.Parameter('')
    
    # TODO: These complete() methods should be abstracted out into more highly
    #       structured parent classes.
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view/table is populated (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM example1;' % self._aspect)
            if cur.rowcount > 0:
                completed = True
        except:
            pass
        
        conn.commit()
        conn.close()
        
        return completed


class OracleExampleTask2(OracleSQLFileTask):
    
    _sql_file_path = luigi.Parameter('')
    
    def requires(self):
        return OracleExampleTask1(_db=self._db)
    
    # TODO: These complete() methods should be abstracted out into more highly
    #       structured parent classes.
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view/table is populated (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM example2;' % self._aspect)
            if cur.rowcount > 0:
                completed = True
        except:
            pass
        
        conn.commit()
        conn.close()
        
        return completed
        


class OracleExampleTask3(OracleSQLFileTask):
    
    _sql_file_path = luigi.Parameter('')
    
    # TODO: These complete() methods should be abstracted out into more highly
    #       structured parent classes.
    def complete(self):
        conn = self.get_conn()
        cur = conn.cursor()
        
        # Check that view/table is populated (low-bar test for completeness)
        completed = False
        try:
            cur.execute('SELECT * FROM example3;' % self._aspect)
            if cur.rowcount > 0:
                completed = True
        except:
            pass
        
        conn.commit()
        conn.close()
        
        return completed


class ExampleWrapperTask(luigi.WrapperTask):
    
    db_host = luigi.Parameter(default='localhost')
    db_port = luigi.Parameter(default='1521')
    db_user = luigi.Parameter()
    db_pass = luigi.Parameter()
    db_name = luigi.Parameter()

    def requires(self):
        db = {
            'host':self.db_host,
            'port':self.db_port,
            'user':self.db_user,
            'pass':self.db_pass,
            'name':self.db_name,
            
        }
        
        yield OracleExampleTask2(_db=db)
        yield OracleExampleTask3(_db=db)
