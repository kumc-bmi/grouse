'''cms_scalable_i2p -- i2b2 to PCORNet CDM optimized for CMS

TODO: migrate relevant module docs from cms_i2p


clues from:
https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py
'''

from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
import luigi

import param_val as pv


class HelloCDM(PySparkTask):
    '''Verify connection to CDM DB.
    '''
    driver_memory = '2g'
    executor_memory = '3g'

    schema = pv.StrParam(default='cms_pcornet_cdm')
    save_path = pv.StrParam(default='/tmp/cdm-harvest.csv')

    db_url = pv.StrParam(description='see client.cfg')
    driver = pv.StrParam(default="oracle.jdbc.OracleDriver")
    user = pv.StrParam(description='see client.cfg')
    passkey = pv.StrParam(description='see client.cfg',
                          significant=False)

    @property
    def __password(self):
        from os import environ  # ISSUE: ambient
        return environ[self.passkey]

    def output(self):
        return luigi.LocalTarget(self.save_path)

    def main(self, sparkContext, *_args):
        spark = SparkSession(sparkContext)
        harvest = spark.read.jdbc(
            table='{t.schema}.harvest'.format(t=self),
            url=self.db_url,
            properties={
                "user": self.user,
                "password": self.__password,
                "driver": self.driver,
            })
        harvest.write.save(self.output().path, format='csv')
