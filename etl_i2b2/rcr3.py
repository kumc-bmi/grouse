"""rcr3 -- ETL tasks to support CancerRCR#Aim3
"""
from datetime import datetime
from typing import List

from sqlalchemy.exc import DatabaseError
import luigi
import pandas as pd  # type: ignore

from script_lib import Script
from sql_syntax import Environment, Params
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

    def script_params(self, conn: et.LoggedConnection) -> Params:
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
    def source(self) -> 'SiteI2B2':
        return SiteI2B2(star_schema=self.site_star_schema)

    @property
    def variables(self) -> Environment:
        return dict(
            I2B2_STAR=self.project.star_schema,
            I2B2_STAR_SITE=self.source.star_schema,
        )

    def script_params(self, conn: et.LoggedConnection) -> Params:
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

    def requires(self) -> List[luigi.Task]:
        table_names = (
            [
                'mbsf_ab_summary',
                'medpar_all',
                'bcarrier_claims',
                'bcarrier_line',
                'outpatient_base_claims',
                'outpatient_revenue_center',
                'pde',
            ] if list(self.cms_rif_schemas) == ['CMS_DEID'] else
            [
                'mbsf_abcd_summary',
                'table medpar_all',
                'bcarrier_claims_k',
                'bcarrier_line_k',
                'outpatient_base_claims_k',
                'outpatient_revenue_center_k',
                'pde',
            ])

        return [
            CohortRIFTable(cms_rif_schemas=self.cms_rif_schemas,
                           work_schema=self.work_schema,
                           site_star_list=self.site_star_list,
                           table_name=table_name)
            for table_name in table_names
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


class CMS_CDM_Report(et.DBAccessTask, et.I2B2Task):
    '''Make a report (spreadsheet) detailing how a CMS_CDM was produced.
    '''

    path = pv.StrParam(default='cms_cdm_report.xlsx')

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(self.path)

    def run(self) -> None:
        query = rif_etl.read_sql_step
        with self.connection('reporting') as lc:
            # ISSUE: sync with build_cohort.sql?
            # ISSUE: left out 'NAACCR|400:C509'
            inclusion_criteria = query('''
            select concept_cd, min(name_char) name_char
            from blueherondata_kumc_calamus.concept_dimension
            where concept_cd in (
              'SEER_SITE:26000',
    	      'NAACCR|400:C500',
              'NAACCR|400:C501',
              'NAACCR|400:C502',
              'NAACCR|400:C503',
              'NAACCR|400:C504',
              'NAACCR|400:C505',
              'NAACCR|400:C506',
              'NAACCR|400:C507',
              'NAACCR|400:C508'
            )
            group by concept_cd
            order by concept_cd
            ''', lc).set_index('concept_cd')

            cohorts = query('''
                select site_schema, result_instance_id, start_date, task_id, count(distinct patient_num)
                from site_cohorts
                group by site_schema, result_instance_id, start_date, task_id
                order by start_date desc
            ''', lc)
            cohorts = cohorts.append(query('''
                select count(distinct site_schema) site_schema
                     , max(result_instance_id) result_instance_id
                     , max(start_date) start_date
                     , 'Total' task_id
                     , count(distinct patient_num)
                 from site_cohorts''', lc)).set_index('task_id')
            cohort_rif_summary = query('select * from cohort_rif_summary', lc)
            i2b2_bc = query('select * from i2b2_bc_summary', lc)

            uploads = query('''
                 select *
                from upload_status up
                where load_status = 'OK' and ((
                      loaded_record > 0
                  and substr(transform_name, -11) in (
                    select distinct task_id from site_cohorts
                  )
                ) or (
                  message like 'UP#%' and upload_label like '% #1 of 1%'
                ))
                order by load_date desc
                ''', lc).set_index('upload_id')

            harvest = query('select * from harvest', lc).transpose();
            dem = query('select * from demographic_summary', lc);
            enc_iid = query('select * from encounters_per_visit_patient', lc)  # Table IIID
            enr = query('select * from id_counts_by_table', lc)       #  -- just ENROLLMENT for now
            dx = query('select * from dx_by_enc_type', lc)            #  -- Table IVA
            px = query('select * from px_per_enc_by_type', lc)        # -- Table IVB
            disp = query('select * from dispensing_trend_chart', lc)  # -- Chart IF

        # ISSUE: This isn't atomic like LocalTarget is supposed to be.
        writer = pd.ExcelWriter(self.path)
        inclusion_criteria.to_excel(writer, 'Inclusion Criteria')
        cohorts.to_excel(writer, 'Site Cohorts')
        cohort_rif_summary.to_excel(writer, 'CMS RIF BC')
        i2b2_bc.to_excel(writer, 'I2B2 BC')
        dem.to_excel(writer, 'DEMOGRAPHIC')
        enc_iid.to_excel(writer, 'ENCOUNTER IID')
        enr.to_excel(writer, 'ENROLLMENT ID')
        dx.to_excel(writer, 'DIAGNOSIS IVA')
        px.to_excel(writer, 'PROCEDURES IVB')
        disp.to_excel(writer, 'DISPENSING IF')
        uploads.to_excel(writer, 'I2B2 Tasks')
        harvest.to_excel(writer, 'Harvest')
        writer.save()


class DateShiftFixAll(luigi.Task):
    parts = {
        # ISSUE: CMS_DEID_2014 was done manually
        'CMS_DEID_2015': (18624, 18630)  # observation_fact_18624 thru observation_fact_18630
    }

    def requires(self) -> List[luigi.Task]:
        return [
            DateShiftFixPart(cms_rif_schema=schema, upload_id=upload_id)
            for schema in self.parts.keys()
            for lo, hi in [self.parts[schema]]
            for upload_id in range(lo, hi + 1)
        ]


class DateShiftFixPart(et.SqlScriptTask):
    script = Script.date_shift_2015_part
    cms_rif_schema = pv.StrParam(default='CMS_DEID_2015')
    upload_id = pv.IntParam()

    @property
    def variables(self) -> Environment:
        return dict(upload_id=str(self.upload_id))
