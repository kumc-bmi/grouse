'''
Usage:

  (grouse-etl)$ python demographics.py

or:

  (grouse-etl)$ PYTHONPATH=. luigi --module demographics Demographics \
                                   --local-scheduler

TODO: why won't luigi find modules in the current directory?

'''

from sqlalchemy.exc import DatabaseError
import luigi

from etl_tasks import DBAccessTask, UploadTask
from script_lib import Script, I2B2STAR

# TODO: get this from output of staging task
CMS_CCW = 'ccwdata.org'  # TODO: sync with cms_ccw_spec.sql


class DemographicsTask(DBAccessTask, luigi.WrapperTask):
    star_schema = luigi.Parameter(default='NIGHTHERONDATA')
    project_id = luigi.Parameter(default='GROUSE')

    def variables(self):
        return {I2B2STAR: self.star_schema}

    def patient_mapping_task(self):
        return UploadTask(script=Script.cms_patient_mapping,
                          source_cd=CMS_CCW,
                          variables=self.variables(),
                          project_id=self.project_id,
                          password=self.password,
                          account=self.account)

    def rollback(self):
        script = Script.cms_patient_mapping
        upload = self.patient_mapping_task().output()
        tables = script.inserted_tables(self.variables())
        objects = script.created_objects()

        with self.output().engine.begin() as work:
            upload.update(load_status=None, end_date=False)

            for _dep, table_name in tables:
                work.execute('truncate table {t}'.format(t=table_name))

            for (_s, (ty, name)) in objects:
                try:
                    work.execute('drop {ty} {name}'.format(ty=ty, name=name))
                except DatabaseError:
                    pass


class Demographics(DemographicsTask):
    def requires(self):
        return self.patient_mapping_task()


class DemographicsRollback(DemographicsTask):
    def complete(self):
        return False

    def run(self):
        self.rollback()


if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
