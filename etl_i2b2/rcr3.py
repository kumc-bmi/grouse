"""rcr3 -- ETL tasks to support CancerRCR#Aim3
"""
from datetime import datetime
from typing import List

from sqlalchemy.exc import DatabaseError
import luigi

from script_lib import Script
from sql_syntax import Environment
import cms_etl
import cms_pd as rif_etl
import etl_tasks as et
import param_val as pv

DateParam = pv._valueOf(datetime(2001, 1, 1, 0, 0, 0), luigi.DateParameter)
ListParam = pv._valueOf(['abc'], luigi.ListParameter)


class CohortDatamart(cms_etl.FromCMS, et.UploadTask):
    site_star_list = ListParam(description='DATA_KUMC,DATA_MCW,...')
    script = Script.cohort_i2b2_datamart

    def requires(self) -> List[luigi.Task]:
        return [just_task for just_task in self._cohort_tasks()]

    def _cohort_tasks(self) -> List['BuildCohort']:
        return [BuildCohort(site_star_schema=star_schema)
                for star_schema in self.site_star_list]

    @property
    def variables(self) -> Environment:
        return dict(
            I2B2_STAR=self.project.star_schema,
        )

    def script_params(self, conn: et.LoggedConnection) -> Environment:
        upload_params = et.UploadTask.script_params(self, conn)

        cohort_ids = [cohort_task.result_instance_id(conn)
                      for cohort_task in self._cohort_tasks()]
        return dict(upload_params,
                    cohort_id_list=','.join(str(i) for i in cohort_ids))


class BuildCohort(et.UploadTask):
    script = Script.build_cohort
    site_star_schema = pv.StrParam(description='DATA_KUMC or DAT_MCW etc.')
    inclusion_concept_cd = pv.StrParam(default='SEER_SITE:26000')
    dx_date_min = DateParam(default=datetime(2011, 1, 1, 0, 0, 0))
    _keys = None
    _key_seqs = ['QT_SQ_QRI_QRIID',
                 'QT_SQ_QM_QMID',
                 'QT_SQ_QRI_QRIID']  # ISSUE: same sequence?

    @property
    def source(self) -> et.SourceTask:
        return SiteI2B2(star_schema=self.site_star_schema)

    @property
    def variables(self) -> Environment:
        return dict(
            I2B2_STAR=self.project.star_schema,
            I2B2_STAR_SITE=self.source.star_schema,
        )

    def script_params(self, conn: et.LoggedConnection) -> Environment:
        upload_params = et.UploadTask.script_params(self, conn)

        [result_instance_id,
         query_master_id,
         query_instance_id] = self._allocate_keys(conn)
        return dict(upload_params,
                    task_id=self.task_id,
                    inclusion_concept_cd=self.inclusion_concept_cd,
                    dx_date_min=self.dx_date_min,
                    result_instance_id=result_instance_id,
                    query_instance_id=query_instance_id,
                    query_master_id=query_master_id,
                    query_name='%s: %s' % (self.site_star_schema, query_master_id),
                    user_id=self.task_family)

    def _allocate_keys(self, conn: et.LoggedConnection) -> List[int]:
        if self._keys is not None:
            return self._keys
        self._keys = out = []  # type: List[int]
        for seq_name in self._key_seqs:
            x = conn.execute("select {schema}.{seq_name}.nextval from dual".format(
                schema=self.project.star_schema, seq_name=seq_name)).scalar()
            out.append(x)
        return out

    def loaded_record(self, conn: et.LoggedConnection, _bulk_rows: int) -> int:
        assert self._keys
        [result_instance_id, _, _] = self._keys
        size = conn.execute(
            '''
            select set_size from {i2b2_star}.qt_query_result_instance
            where result_instance_id = :result_instance_id
            '''.format(i2b2_star=self.project.star_schema),
            dict(result_instance_id=result_instance_id)
        ).scalar()  # type: int
        return size

    def result_instance_id(self, conn: et.LoggedConnection) -> int:
        '''Fetch patient set id after task has run.
        '''
        rid = conn.execute(
            '''
            select max(result_instance_id)
            from {i2b2_star}.qt_query_result_instance
            where set_size > 0 and description = :task_id
            '''.format(i2b2_star=self.project.star_schema),
            params=dict(task_id=self.task_id)
        ).scalar()  # type: int
        return rid


class SiteI2B2(et.SourceTask, et.DBAccessTask):
    star_schema = pv.StrParam(description='BLUEHERONDATA_KUMC or the like')
    table_eg = 'patient_dimension'

    @property
    def source_cd(self) -> str:
        return "'%s'" % self.star_schema

    @property
    def download_date(self) -> datetime:
        with self.connection('download_date') as conn:
            t = conn.execute(
                '''
                select last_ddl_time from all_objects
                where owner=:owner and object_name=:table_eg
                ''',
                dict(owner=self.star_schema.upper(), table_eg=self.table_eg.upper())
            ).scalar()  # type: datetime
        return t

    def _dbtarget(self) -> et.DBTarget:
        return et.SchemaTarget(self._make_url(self.account),
                               schema_name=self.star_schema,
                               table_eg=self.table_eg,
                               echo=self.echo)


class CohortRIF(luigi.WrapperTask):
    """Build subset CMS RIF tables where bene_id is from site cohorts
    in i2b2 patient sets.

    ISSUE: how to handle difference between mbsf_ab_summary (2011-2013) and mbsf_abcd_summary (14, 15)?
    """
    cms_rif_schemas = ListParam(default=['CMS_DEID_2014', 'CMS_DEID_2015'])
    site_star_list = ListParam(description='DATA_KUMC,DATA_MCW,...')
    work_schema = pv.StrParam()

    table_names = [
        rif_etl.MBSFUpload.table_name,
        rif_etl.MEDPAR_Upload.table_name,
        rif_etl.CarrierClaimUpload.table_name,
        rif_etl.CarrierLineUpload.table_name,
        rif_etl.OutpatientClaimUpload.table_name,
        rif_etl.OutpatientRevenueUpload.table_name,
        rif_etl.OutpatientRevenueUpload.table_name,
        rif_etl.DrugEventUpload.table_name,
    ]

    def requires(self) -> List[luigi.Task]:
        return [
            CohortRIFTable(cms_rif_schemas=self.cms_rif_schemas,
                           work_schema=self.work_schema,
                           site_star_list=self.site_star_list,
                           table_name=table_name)
            for table_name in self.table_names
        ]


class CohortRIFTable(luigi.WrapperTask):
    cms_rif_schemas = ListParam(default=['CMS_DEID_2014', 'CMS_DEID_2015'])
    work_schema = pv.StrParam(description="Destination RIF schema")
    table_name = pv.StrParam()
    site_star_list = ListParam(description='DATA_KUMC,DATA_MCW,...')

    def requires(self) -> List[luigi.Task]:
        return [
            CohortRIFTablePart(
                cms_rif=cms_rif,
                work_schema=self.work_schema,
                site_star_list=self.site_star_list,
                table_name=self.table_name)
            for cms_rif in self.cms_rif_schemas
        ]


class SiteCohorts(luigi.WrapperTask):
    """Wrapper around a collection of site cohorts

    This avoids M*N dependencies in the luigi task visualizer,
    where M=number of tables and N=number of sites.
    """
    site_star_list = ListParam(description='DATA_KUMC,DATA_MCW,...')

    def requires(self) -> List[luigi.Task]:
        return [t for t in self._cohort_tasks()]

    def _cohort_tasks(self) -> List['BuildCohort']:
        return [BuildCohort(site_star_schema=star_schema)
                for star_schema in self.site_star_list]

    def _cohort_ids(self, i2b2_star: str, conn: et.LoggedConnection) -> List[int]:
        cohort_tasks = self._cohort_tasks()
        cohort_ids = [
            conn.execute('''
            select max(result_instance_id)
            from {i2b2}.qt_query_result_instance ri
            where ri.description = :task_id
            '''.format(i2b2=i2b2_star),
                         params=dict(task_id=task.task_id)).scalar()
            for task in cohort_tasks
        ]
        return cohort_ids


class CohortRIFTablePart(et.DBAccessTask, et.I2B2Task):
    """Build subset of one CMS RIF table where bene_id is from site cohorts.
    """
    cms_rif = pv.StrParam(description="Source RIF schema")
    work_schema = pv.StrParam(description="Destination RIF schema")
    table_name = pv.StrParam()
    site_star_list = ListParam(description='DATA_KUMC,DATA_MCW,...')
    parallel_degree = pv.IntParam(significant=False, default=16)

    @property
    def site_cohorts(self) -> SiteCohorts:
        return SiteCohorts(site_star_list=self.site_star_list)

    def requires(self) -> List[luigi.Task]:
        return [self.site_cohorts]

    def complete(self) -> bool:
        for t in self.requires():
            if not t.complete():
                return False

        with self.connection('rif_table_done') as conn:
            for cohort_id in self.site_cohorts._cohort_ids(self.project.star_schema, conn):
                # We're not guaranteed that each site cohort intersects the CMS data,
                # but if it does, the CMS patient numbers are the low ones; hence min().
                lo, hi = conn.execute('''
                    select min(patient_num), max(patient_num)  from {i2b2}.qt_patient_set_collection
                    where result_instance_id = {id}
                    '''.format(i2b2=self.project.star_schema, id=cohort_id)).first()
                try:
                    found = conn.execute('''
                        select 1 from {work}.{t} where bene_id between :lo and :hi and rownum = 1
                    '''.format(work=self.work_schema, t=self.table_name),
                                         params=dict(lo=lo, hi=hi)).scalar()
                except DatabaseError as oops:
                    conn.log.warn('complete query failed:', exc_info=oops)
                    return False
                if not found:
                    return False
        return True

    def run(self) -> None:
        with self.connection('rif_table') as conn:
            self._create(conn)

            cohort_ids = self.site_cohorts._cohort_ids(self.project.star_schema, conn)
            conn.execute('''
            insert /*+ append */ into {work}.{t}
            select /*+ parallel({degree}) */ * from {rif}.{t}
            where bene_id in (
              select patient_num from {i2b2}.qt_patient_set_collection
              where result_instance_id in ({cohorts})
            )
            '''.format(work=self.work_schema, t=self.table_name,
                       degree=self.parallel_degree, rif=self.cms_rif,
                       i2b2=self.project.star_schema,
                       cohorts=', '.join(str(i) for i in cohort_ids)
                       ))

    def _create(self, conn: et.LoggedConnection) -> None:
        try:
            conn.execute('''
            create table {work}.{t} as select * from {rif}.{t} where 1 = 0
            '''.format(work=self.work_schema, t=self.table_name,
                       rif=self.cms_rif))
        except DatabaseError:
            pass  # perhaps it already exists...
