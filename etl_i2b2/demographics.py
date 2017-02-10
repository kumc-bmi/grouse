'''
Usage:

  (grouse-etl)$ python demographics.py

or:

  (grouse-etl)$ PYTHONPATH=. luigi --module demographics Demographics \
                                   --local-scheduler

TODO: why won't luigi find modules in the current directory?

'''

import luigi

from etl_tasks import DBAccessTask, UploadTask, UploadRollback
from script_lib import Script, I2B2STAR

# TODO: get this from output of staging task
CMS_CCW = 'ccwdata.org'  # TODO: sync with cms_ccw_spec.sql


class _GrouseTask(luigi.WrapperTask):
    star_schema = luigi.Parameter(default='NIGHTHERONDATA')
    project_id = luigi.Parameter(default='GROUSE')
    source_cd = luigi.Parameter(default=CMS_CCW)

    @property
    def variables(self):
        return {I2B2STAR: self.star_schema}


class PatientMappingTask(UploadTask, _GrouseTask):
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


class PatientDimensionTask(UploadTask, _GrouseTask):
    script = Script.cms_patient_dimension

    def requires(self):
        return _make_from(PatientMappingTask, self)


class Demographics(DBAccessTask, _GrouseTask):
    def requires(self):
        return _make_from(PatientDimensionTask, self)


class DemographicsRollback(DBAccessTask, _GrouseTask):
    def complete(self):
        return False

    def run(self):
        for script in [PatientDimensionTask.script,
                       PatientMappingTask.script]:
            task = _make_from(UploadRollback, self,
                              script=script)
            task.rollback()


if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
