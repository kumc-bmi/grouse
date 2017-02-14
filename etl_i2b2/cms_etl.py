'''
See luigi.cfg.example for usage info.

Integration Test Usage:

  (grouse-etl)$ python demographics.py

'''

import luigi

from etl_tasks import (
    DBAccessTask, SqlScriptTask, UploadTask, ReportTask,
    TimeStampParameter,
    UploadRollback)
from script_lib import Script, I2B2STAR

# TODO: get this from output of staging task
CMS_CCW = 'ccwdata.org'  # TODO: sync with cms_ccw_spec.sql


class CMSExtract(luigi.Config):
    download_date = TimeStampParameter()


class GrouseTask(luigi.Task):
    star_schema = luigi.Parameter(default='NIGHTHERONDATA')
    project_id = luigi.Parameter(default='GROUSE')
    source_cd = luigi.Parameter(default=CMS_CCW)
    download_date = TimeStampParameter(default=CMSExtract().download_date)

    @property
    def variables(self):
        return {I2B2STAR: self.star_schema}


class GrouseWrapper(luigi.WrapperTask, GrouseTask):
    pass


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
        return _make_from(PatientMappingTask, self)


class DemographicSummaryReport(ReportTask, GrouseTask):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self):
        data = _make_from(PatientDimensionTask, self)
        report = _make_from(SqlScriptTask, self,
                            script=self.script)
        return [data, report]


class Demographics(DBAccessTask, GrouseWrapper):
    def requires(self):
        return _make_from(DemographicSummaryReport, self)


class DemographicsRollback(DBAccessTask, GrouseWrapper):
    def complete(self):
        return False

    def run(self):
        # ISSUE: use requires() instead?
        for script in [
                # ISSUE: this one isn't actually an UploadTask
                DemographicSummaryReport.script,
                PatientDimensionTask.script,
                PatientMappingTask.script]:
            task = _make_from(UploadRollback, self,
                              script=script)
            task.rollback()


if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
