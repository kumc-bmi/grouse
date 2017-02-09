'''
Usage:

  (grouse-etl)$ python demographics.py

or:

  (grouse-etl)$ PYTHONPATH=. luigi --module demographics Demographics \
                                   --local-scheduler

TODO: why won't luigi find modules in the current directory?

'''

import luigi
from luigi.contrib import sqla


class Demographics(luigi.Task):
    cdw_account = luigi.Parameter(default='sqlite:///')
    i2b2star_schema = luigi.Parameter(default='NIGHTHERONDATA')
    echo = luigi.BoolParameter(default=True)  # TODO: proper logging

    def run(self):
        pass

    def output(self):
        return sqla.SQLAlchemyTarget(
            connection_string=self.cdw_account,
            target_table='%s.upload_status' % self.i2b2star_schema,
            update_id=self.__class__.__name__,
            echo=self.echo)


if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
