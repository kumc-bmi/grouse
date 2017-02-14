'''
See luigi.cfg.example for usage info.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

import luigi

from etl_tasks import (
    DBAccessTask, SqlScriptTask, UploadTask, ReportTask,
    TimeStampParameter)
from script_lib import Script, I2B2STAR


class CMSExtract(luigi.Config):
    download_date = TimeStampParameter()
    source_cd = luigi.Parameter()


class GrouseTask(luigi.Task):
    star_schema = luigi.Parameter(default='NIGHTHERONDATA')  # ISSUE: get from I2B2Project task?
    project_id = luigi.Parameter(default='GROUSE')
    source_cd = luigi.Parameter(default=CMSExtract().source_cd)
    download_date = TimeStampParameter(default=CMSExtract().download_date)

    @property
    def variables(self):
        return {I2B2STAR: self.star_schema}


class GrouseWrapper(luigi.WrapperTask, GrouseTask):
    pass


class GrouseETL(DBAccessTask, GrouseWrapper):
    def parts(self):
        return [
            _make_from(Demographics, self),
            _make_from(Encounters, self),
            _make_from(DiagnosesLoad, self),
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


class PatientMappingTask(UploadTask, GrouseWrapper):
    script = Script.cms_patient_mapping


def _make_from(dest_class, src, **kwargs):
    names = dest_class.get_param_names(include_significant=True)
    src_args = src.param_kwargs
    kwargs = dict((name,
                   kwargs[name] if name in kwargs else
                   src_args[name] if name in src_args else
                   getattr(src, name))
                  for name in names)
    return dest_class(**kwargs)


class PatientDimensionTask(UploadTask, GrouseWrapper):
    script = Script.cms_patient_dimension

    def requires(self):
        return UploadTask.requires(self) + [
            _make_from(PatientMappingTask, self)]


class DemographicSummaryReport(ReportTask, GrouseTask):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self):
        return dict(
            data=_make_from(PatientDimensionTask, self),
            report=_make_from(SqlScriptTask, self,
                              script=self.script))

    def rollback(self):
        self.requires()['data'].rollback()


class Demographics(DBAccessTask, GrouseWrapper):
    def requires(self):
        return _make_from(DemographicSummaryReport, self)

    def rollback(self):
        self.requires().rollback()


class EncounterMappingTask(UploadTask, GrouseWrapper):
    script = Script.cms_encounter_mapping


class VisitDimensionTask(UploadTask, GrouseWrapper):
    script = Script.cms_visit_dimension

    def requires(self):
        return UploadTask.requires(self) + [
            _make_from(PatientMappingTask, self),
            _make_from(EncounterMappingTask, self)]


class EncounterReport(ReportTask, GrouseTask):
    script = Script.cms_enc_dstats
    report_name = 'encounters_per_visit_patient'

    def requires(self):
        return dict(
            data=_make_from(VisitDimensionTask, self),
            report=_make_from(SqlScriptTask, self,
                              script=self.script))

    def rollback(self):
        self.requires()['data'].rollback()


class Encounters(DBAccessTask, GrouseWrapper):
    def requires(self):
        return _make_from(EncounterReport, self)

    def rollback(self):
        self.requires().rollback()


class DiagnosesTransform(SqlScriptTask, GrouseWrapper):
    script = Script.cms_dx_txform


class DiagnosesLoad(UploadTask, GrouseWrapper):
    script = Script.cms_facts_load
    # ISSUE: PatientDimensionTask is getting duplicated,
    #        once with fact_view and once without.
    fact_view = 'observation_fact_cms_dx'

    @property
    def label(self):
        return DiagnosesTransform.script.title

    @property
    def transform_name(self):
        return self.fact_view

    @property
    def variables(self):
        return dict(GrouseWrapper().variables,
                    fact_view=self.fact_view)

    def requires(self):
        return [
            _make_from(DiagnosesTransform, self),
            _make_from(PatientMappingTask, self),
            _make_from(EncounterMappingTask, self),
        ]

if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
