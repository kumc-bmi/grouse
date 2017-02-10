'''
Usage:

  (grouse-etl)$ python demographics.py

or:

  (grouse-etl)$ PYTHONPATH=. luigi --module demographics Demographics \
                                   --local-scheduler

TODO: why won't luigi find modules in the current directory?

'''

import luigi

from script_lib import Script
from etl_tasks import UploadTask, AccountParameter
from etl_tasks import CMS_CCW, I2B2STAR


class Demographics(luigi.Task):
    cdw_account = AccountParameter()
    i2b2star_schema = luigi.Parameter(default='NIGHTHERONDATA')

    def requires(self):
        return [UploadTask(script=Script.cms_patient_mapping,
                           source_cd=CMS_CCW,
                           variables={I2B2STAR: self.i2b2star_schema},
                           account=self.cdw_account)]


if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
