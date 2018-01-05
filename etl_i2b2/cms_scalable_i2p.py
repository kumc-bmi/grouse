'''cms_scalable_i2p -- i2b2 to PCORNet CDM optimized for CMS

TODO: migrate relevant module docs from cms_i2p


clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
'''

from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
import luigi

import param_val as pv


class JDBC4ETL(luigi.Task):
    '''Config for JDBC connection for ETL.
    '''
    db_url = pv.StrParam(description='see client.cfg')
    driver = pv.StrParam(default="oracle.jdbc.OracleDriver")
    user = pv.StrParam(description='see client.cfg')
    passkey = pv.StrParam(description='see client.cfg',
                          significant=False)

    @property
    def __password(self):
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def table_access(self, spark_rw, name):
        return spark_rw.jdbc(
            table=name,
            url=self.db_url,
            properties={
                "user": self.user,
                "password": self.__password,
                "driver": self.driver,
            })


class HelloCDM(PySparkTask):
    '''Verify connection to CDM DB.
    '''
    driver_memory = '2g'
    executor_memory = '3g'

    schema = pv.StrParam(default='cms_pcornet_cdm')
    save_path = pv.StrParam(default='/tmp/cdm-harvest.csv')

    @property
    def db(self):
        return JDBC4ETL()

    def output(self):
        return luigi.LocalTarget(self.save_path)

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        harvest = self.db.table_access(
            spark.read, '{t.schema}.harvest'.format(t=self))
        harvest.write.save(self.output().path, format='csv')


class ProceduresLoad(PySparkTask):

    save_path = pv.StrParam(default='/tmp/px_meta.csv')

    @property
    def db(self):
        return JDBC4ETL()

    def output(self):
        return luigi.LocalTarget(self.save_path)

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        pcornet_proc = self.db.table_access(
            spark.read, 'grousemetadata.pcornet_proc')  # TODO: input()
        pcornet_proc.createOrReplaceTempView('pcornet_proc')
        px_meta = spark.sql(PX_META_SQL.format(pcornet_proc='pcornet_proc'))
        px_meta.write.save(self.output().path, format='csv')
        # TODO: the rest...


PX_META_SQL = r'''
select c_basecode concept_cd
     , SUBSTR(pr.pcori_basecode, INSTR(pr.pcori_basecode, ':') + 1, 11) px
     -- C4 and HC got merged as CH in CDM 3.1
     , regexp_replace(regexp_replace(
         SUBSTR(pr.c_fullname, length('-PCORI-PROCEDURE-%'), 2),
         'C4', 'CH'), 'HC', 'CH') px_type
     , c_name
from {pcornet_proc} pr
where pr.c_fullname like '\PCORI\PROCEDURE\%'
  and pr.c_synonym_cd = 'N'
  and pcori_basecode is not null
  and pr.c_basecode not like 'PROCEDURE:%'
  and pr.m_applied_path = '@'
  and pr.c_visualattributes like 'L%'
'''.replace('\\', '\\' * 4)  # ISSUE: C-style escapes

