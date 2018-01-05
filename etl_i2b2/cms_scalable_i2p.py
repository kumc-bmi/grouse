'''cms_scalable_i2p -- i2b2 to PCORNet CDM optimized for CMS

TODO: migrate relevant module docs from cms_i2p



clues from:
https://cwiki.apache.org/confluence/display/Hive/LanguageManual

https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
'''

from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
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

    def read(self, spark):
        return (spark.read.format('jdbc')
                .options(
                    url=self.db_url,
                    user=self.user,
                    password=self.__password,
                    driver=self.driver,
                    fetchsize=self.fetchsize,
                    batchsize=self.batchsize))


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
        harvest = self.db.read(spark).options(
            dbtable='{t.schema}.harvest'.format(t=self)).load()
        harvest.write.save(self.output().path, format='csv')


class ProceduresLoad(PySparkTask):

    save_path = pv.StrParam(default='/tmp/procedures')
    i2b2_star = pv.StrParam(default='grousedata')
    cdm = pv.StrParam(default='cms_pcornet_cdm')
    pat_group_qty = pv.IntParam(default=1000, significant=False)

    proc_pat = r'(^(CPT|HCPCS|ICD10PCS):)|(^ICD9:\d{2}\.\d{1,2})'

    @property
    def db(self):
        return JDBC4ETL()

    def output(self):
        return luigi.LocalTarget(self.save_path)  # ISSUE: ambient

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)

        #@@ groups = self.patient_groups(spark).collect()
        patParts = self.db.read(spark).options(
            partitionColumn='patient_num',
            numPartitions=self.pat_group_qty,
            #@@lowerBound=groups[0].PATIENT_NUM_LO,
            #@@upperBound=groups[-1].PATIENT_NUM_HI
            lowerBound=0,
            upperBound=25000000,
        )
        obs = patParts.options(
            dbtable='''
            (select * from {t.i2b2_star}.observation_fact
            where regexp_like(concept_cd, '{t.proc_pat}')
            and rownum < 1000  -- TODO@@@
            )
            '''.format(t=self)).load()
        obs.printSchema()
        obs.createOrReplaceTempView('observation_fact')

        enc = patParts.options(dbtable='{t.cdm}.encounter'.format(t=self)).load()
        enc.createOrReplaceTempView('encounter')

        self.px_meta(spark).createOrReplaceTempView('px_meta')

        proc = spark.sql(
            PROCEDURES_SQL.format(observation_fact='observation_fact',
                                  px_meta='px_meta',
                                  encounter='encounter'))

        proc = proc.withColumn('proceduresid', fun.monotonically_increasing_id())
        proc.explain()
        # proc.show(20)
        # proc.write.partitionBy('patid').format('parquet').save(self.output().path)

    def px_meta(self, spark):
        meta = self.db.read(spark).options(
            dbtable='grousemetadata.pcornet_proc').load()
        meta.createOrReplaceTempView('pcornet_proc')
        px_meta = spark.sql(PX_META_SQL.format(pcornet_proc='pcornet_proc'))
        #@@px_meta.createOrReplaceTempView('px_meta')
        return px_meta.cache()

    pat_grp_subq = '''
    (
        select group_num
             , min(patient_num) patient_num_lo
             , max(patient_num) patient_num_hi
        from (
          select patient_num
               , ntile({group_qty}) over (order by patient_num) as group_num
          from (
            select /*+ parallel(20) */ distinct patient_num from {patient_dimension}
            where patient_num is not null
          ) ea
        ) w_ntile
        group by group_num
        order by group_num
    )
    '''

    def patient_groups(self, spark):
        pd_name = '{t.i2b2_star}.patient_dimension'.format(t=self)
        groups = self.db.read(spark).options(
            dbtable=self.pat_grp_subq.format(patient_dimension=pd_name,
                                             group_qty=self.pat_group_qty))
        return groups.load().cache()


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
select obs.patient_num PATID
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
