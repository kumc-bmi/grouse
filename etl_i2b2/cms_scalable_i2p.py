'''cms_scalable_i2p -- i2b2 to PCORNet CDM optimized for CMS

TODO: migrate relevant module docs from cms_i2p



clues from:
https://cwiki.apache.org/confluence/display/Hive/LanguageManual

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

    fetchsize = pv.IntParam(default=10000, significant=False)
    batchsize = pv.IntParam(default=10000, significant=False)

    @property
    def __password(self):
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def table_access(self, spark_rw, name,
                     partitionColumn=None, numPartitions=None):
        properties = dict(user=self.user,
                          password=self.__password,
                          driver=self.driver,
                          fetchsize=self.fetchsize,
                          batchsize=self.batchsize)
        if partitionColumn:
            properties = dict(properties,
                              lowerBound=0,  # ISSUE: compute these dynamically?
                              upperBound=2000000,
                              partitionColumn=partitionColumn,
                              numPartitions=str(numPartitions))
        return spark_rw.jdbc(
            table=name,
            url=self.db_url,
            properties={k: str(v) for k, v in properties.items()})


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
        return luigi.LocalTarget(self.save_path)  # ISSUE: ambient

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        harvest = self.db.table_access(
            spark.read, '{t.schema}.harvest'.format(t=self))
        harvest.write.save(self.output().path, format='csv')


class ProceduresLoad(PySparkTask):

    save_path = pv.StrParam(default='/tmp/procedures')
    i2b2_star = pv.StrParam(default='grousedata')
    cdm = pv.StrParam(default='cms_pcornet_cdm')
    pat_group_qty = pv.IntParam(default=1000, significant=False)

    @property
    def db(self):
        return JDBC4ETL()

    def output(self):
        return luigi.LocalTarget(self.save_path)  # ISSUE: ambient

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)

        detail = {
            'pcornet_proc': ('grousemetadata.pcornet_proc', None, None),
            'observation_fact': ('{t.i2b2_star}.observation_fact'.format(t=self),
                                 'patient_num', self.pat_group_qty),
            'encounter': ('{t.cdm}.encounter'.format(t=self),
                          'patid', self.pat_group_qty)
        }
        for alias, (jdbc_name, partitionColumn, numPartitions) in detail.items():
            df = self.db.table_access(
                spark.read, jdbc_name,
                partitionColumn=partitionColumn, numPartitions=numPartitions)
            df.createOrReplaceTempView(alias)

        px_meta = spark.sql(PX_META_SQL.format(pcornet_proc='pcornet_proc'))
        px_meta.createOrReplaceTempView('px_meta')
        px_meta.cache()

        proc = spark.sql(
            PROCEDURES_SQL.format(observation_fact='observation_fact',
                                  px_meta='px_meta',
                                  encounter='encounter'))
        proc.explain()
        # proc.write.partionBy('patid').format('parquet').save(self.output().path)


# TODO: put SQL back in SQL files
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

PROCEDURES_SQL = r'''
select monotonically_increasing_id() -- obs.upload_id || ' ' || obs.patient_num || ' ' || obs.instance_num PROCEDURESID ???
     , obs.patient_num PATID
     , obs.encounter_num ENCOUNTERID
     , nvl(enc.enc_type, 'NI') ENC_TYPE
     , enc.ADMIT_DATE
     , enc.PROVIDERID
     , obs.start_date PX_DATE
     , px_meta.PX
     , px_meta.PX_TYPE
     , 'CL' PX_SOURCE  -- (select Claim from px_source_enum)
     , px_meta.c_name RAW_PX
     , obs.upload_id RAW_PX_TYPE
from {observation_fact} obs
join {px_meta} px_meta on px_meta.concept_cd = obs.concept_cd
-- ISSUE: prove that these left-joins match at most once.
left join {encounter} enc on obs.patient_num = enc.patid
                         and obs.encounter_num = enc.encounterid
'''
