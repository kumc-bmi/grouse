'''cms_etl -- Load an i2b2 star schema from CMS RIF data

See README.md background and usage.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

import logging

import luigi

from etl_tasks import (
    SqlScriptTask, UploadTask, ReportTask,
    TimeStampParameter)
from script_lib import Script, ChunkByBene
from sql_syntax import param_names
import script_lib as lib

log = logging.getLogger(__name__)


class CMSExtract(luigi.Task):
    download_date = TimeStampParameter(description='see luigi.cfg.example')
    cms_rif = luigi.Parameter(description='see luigi.cfg.example')
    script_variable = 'cms_source_cd'
    source_cd = "'ccwdata.org'"
    bene_chunks = luigi.IntParameter(default=1,
                                     description='see luigi.cfg.example')
    chunk_limit = luigi.IntParameter(default=None,
                                     description='see luigi.cfg.example')

    def complete(self):
        return not not self.download_date


class GrouseETL(luigi.WrapperTask):
    def parts(self):
        return [
            Demographics(),
            Encounters(),
            Diagnoses(),
        ]

    def requires(self):
        return self.parts()


class GrouseRollback(GrouseETL):
    def complete(self):
        return False

    def requires(self):
        return []

    def run(self):
        for task in self.parts():
            task.rollback()


class FromCMS(object):
    '''Mix in source and substitution variables for CMS ETL scripts.

    Note project attribute is supplied by UploadTask.
    '''

    @property
    def source(self):
        return CMSExtract()

    @property
    def vars_for_deps(self):
        config = [(lib.I2B2STAR, self.project.star_schema),
                  (lib.CMS_RIF, self.source.cms_rif)]
        design = [(CMSExtract.script_variable, CMSExtract.source_cd)]
        return dict(config + design)


class _MappingTask(FromCMS, UploadTask):
    def requires(self):
        return UploadTask.requires(self) + [self.source]


class _DimensionTask(FromCMS, UploadTask):
    def requires(self):
        return SqlScriptTask.requires(self) + self.mappings()

    def rollback(self):
        UploadTask.rollback(self)
        for task in self.mappings():
            task.rollback()


class _FactLoadTask(FromCMS, UploadTask):
    script = Script.cms_facts_load

    @property
    def label(self):
        return self.txform.title

    @property
    def transform_name(self):
        return self.fact_view

    @property
    def variables(self):
        return dict(self.vars_for_deps,
                    fact_view=self.fact_view)

    def requires(self):
        mappings = [PatientMapping(), EncounterMapping()]
        txform = SqlScriptTask(
            script=self.txform,
            variables=self.vars_for_deps)
        return SqlScriptTask.requires(self) + mappings + [txform]


class _BeneChunked(FromCMS):
    def chunks(self, names_present):
        if not ChunkByBene.required_params <= set(names_present):
            return [{}]

        qty, limit = self.source.bene_chunks, self.source.chunk_limit

        with self.dbtrx() as q:
            sql = ChunkByBene.chunk_query(qty, chunk_source=self.chunk_source)
            result = q.execute(sql).fetchall()
            chunks, sizes = ChunkByBene.result_chunks(result, limit)
            log.info('chunks: %d limit: %s sizes: %s...',
                     len(result), limit, sizes[:3])
        return chunks

    chunk_source = None


class _DataReport(ReportTask):
    def requires(self):
        return dict(
            data=self.data_task,
            report=SqlScriptTask(script=self.script,
                                 variables=self.data_task.vars_for_deps))

    def rollback(self):
        if self.output().exists():
            self.output().remove()
        for _k, task in self.requires().items():
            task.rollback()


class PatientMapping(_MappingTask):
    script = Script.cms_patient_mapping


class PatientDimension(_DimensionTask):
    script = Script.cms_patient_dimension

    def mappings(self):
        return [PatientMapping()]


class Demographics(_DataReport):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    @property
    def data_task(self):
        return PatientDimension()


class EncounterMapping(_BeneChunked, _MappingTask):
    script = Script.cms_encounter_mapping


class VisitDimension(_DimensionTask):
    script = Script.cms_visit_dimension

    def mappings(self):
        return [PatientMapping(), EncounterMapping()]


class Encounters(_DataReport):
    script = Script.cms_enc_dstats
    report_name = 'encounters_per_visit_patient'

    @property
    def data_task(self):
        return VisitDimension()


class DiagnosesLoad(_BeneChunked, _FactLoadTask):
    fact_view = 'observation_fact_cms_dx'
    txform = Script.cms_dx_txform

    @property
    def chunk_source(self):
        return '''
        (select distinct bene_id from {cms_rif}.bcarrier_claims)
        '''.format(cms_rif=self.source.cms_rif)


class Diagnoses(_DataReport):
    script = Script.cms_dx_dstats
    report_name = 'dx_by_enc_type'

    @property
    def data_task(self):
        return DiagnosesLoad()


if __name__ == '__main__':
    luigi.build([GrouseETL()], local_scheduler=True)
