'''cms_etl -- Load an i2b2 star schema from CMS RIF data

See README.md background and usage.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

import luigi

from etl_tasks import (
    DBAccessTask, SqlScriptTask, UploadTask, ReportTask,
    TimeStampParameter)
from script_lib import Script, I2B2STAR, CMS_RIF


class CMSExtract(luigi.Task):
    download_date = TimeStampParameter()
    cms_rif = luigi.Parameter()
    source_cd = "'ccwdata.org'"
    variable_name = 'cms_source_cd'

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


class _FromCMS(luigi.WrapperTask):
    source = luigi.TaskParameter(CMSExtract())

    @property
    def variables(self):
        return {I2B2STAR: self.project.star_schema,
                CMS_RIF: self.source.cms_rif,
                CMSExtract.variable_name: CMSExtract.source_cd}


class PatientMappingTask(_FromCMS, UploadTask):
    script = Script.cms_patient_mapping


class PatientDimensionTask(_FromCMS, UploadTask):
    script = Script.cms_patient_dimension

    def requires(self):
        return UploadTask.requires(self) + [
            PatientMappingTask(source=self.source)]


class _DataReport(ReportTask, DBAccessTask):
    def requires(self):
        return dict(
            data=self.data_task,
            report=SqlScriptTask(script=self.script,
                                 variables=self.data_task.variables))

    def rollback(self):
        self.requires()['data'].rollback()


class Demographics(_DataReport):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    @property
    def data_task(self):
        return PatientDimensionTask(source=CMSExtract())


class EncounterMappingTask(_FromCMS, UploadTask):
    script = Script.cms_encounter_mapping


class VisitDimensionTask(_FromCMS, UploadTask):
    script = Script.cms_visit_dimension

    def requires(self):
        return UploadTask.requires(self) + [
            PatientMappingTask(source=self.source),
            EncounterMappingTask(source=self.source)]

    def rollback(self):
        UploadTask.rollback(self)
        for task in self.requires()[-2:]:
            if isinstance(task, UploadTask):
                task.rollback()


class Encounters(_DataReport):
    script = Script.cms_enc_dstats
    report_name = 'encounters_per_visit_patient'

    @property
    def data_task(self):
        return VisitDimensionTask(source=CMSExtract())


class DiagnosesTransform(SqlScriptTask, luigi.WrapperTask):
    script = Script.cms_dx_txform


class DiagnosesLoad(_FromCMS, UploadTask):
    script = Script.cms_facts_load
    fact_view = 'observation_fact_cms_dx'

    @property
    def label(self):
        return DiagnosesTransform.script.title

    @property
    def transform_name(self):
        return self.fact_view

    @property
    def variables(self):
        return {I2B2STAR: self.project.star_schema,
                CMS_RIF: self.source.cms_rif,
                CMSExtract.variable_name: CMSExtract.source_cd,
                'fact_view': self.fact_view}

    def requires(self):
        return [
            DiagnosesTransform(variables=self.variables),
            PatientMappingTask(source=self.source),
            EncounterMappingTask(source=self.source),
        ]


class Diagnoses(_DataReport, _FromCMS):
    script = Script.cms_dx_dstats
    report_name = 'dx_by_enc_type'

    @property
    def data_task(self):
        return DiagnosesLoad(source=CMSExtract())


if __name__ == '__main__':
    luigi.build([GrouseETL()], local_scheduler=True)
