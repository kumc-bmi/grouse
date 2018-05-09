"""rcr3 -- ETL tasks to support CancerRCR#Aim3
"""
from datetime import datetime

from script_lib import Script
from sql_syntax import Environment
import etl_tasks as et
import param_val as pv

TimeStampParam = pv._valueOf(datetime(2001, 1, 1, 0, 0, 0), et.TimeStampParameter)


class BuildCohort(et.UploadTask):
    script = Script.build_cohort
    site_id = pv.StrParam(description='KUMC or MCW etc.')
    inclusion_concept_cd = pv.StrParam(default='SEER_SITE:26000')

    @property
    def source(self):
        return SiteI2B2(star_schema='BLUEHERONDATA_' + self.site_id)

    @property
    def variables(self):
        return dict(I2B2_STAR_SITE=self.source.star_schema)

    def run(self) -> None:
        [result_instance_id, query_master_id] = self._allocate_sequence_numbers(
            ['QT_SQ_QRI_QRIID', 'QT_SQ_QM_QMID'])

        et.SqlScriptTask.run_bound(self, script_params=dict(
            task_id=self.task_id,
            inclusion_concept_cd=self.inclusion_concept_cd,
            result_instance_id=result_instance_id,
            query_master_id=query_master_id,
            query_name='%s: %s' % (self.site_id, query_master_id),
            user_id=self.task_family,
            project_id=self.project.project_id))

    def _allocate_sequence_numbers(self, names):
        out = []
        with self.connection('sequence_numbers') as conn:
            for seq_name in names:
                x = conn.execute("select {schema}.{seq_name}.nextval from dual".format(
                    schema=self.project.star_schema, seq_name=seq_name)).scalar()
                out.append(x)
        return out


class SiteI2B2(et.SourceTask, et.DBAccessTask):
    star_schema = pv.StrParam(description='BLUEHERONDATA_KUMC or the like')
    table_eg = 'patient_dimension'

    #@@script_variable = 'cms_source_cd'

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
            ).scalar()
        return t

    def _dbtarget(self) -> et.DBTarget:
        return et.SchemaTarget(self._make_url(self.account),
                               schema_name=self.star_schema,
                               table_eg=self.table_eg,
                               echo=self.echo)
