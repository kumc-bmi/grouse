'''cms_i2p -- i2b2 to PCORNet CDM optimized for CMS
'''

from typing import cast

import luigi

from etl_tasks import DBAccessTask, I2B2Task, SqlScriptTask
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Environment


class I2P(luigi.WrapperTask):
    tables = [
        ('DEMOGRAPHIC', Script.cms_dem_dstats, 'pcornet_demographic'),
        # TODO: code to create the rest of these tables
        ('ENCOUNTER', Script.cms_enc_dstats, 'pcornet_encounter'),
        ('DIAGNOSIS', Script.cms_dx_dstats, 'pcornet_diagnosis'),
    ]

    def requires(self):
        return [
            FillTableFromView(table=table, script=script, view=view)
            for (table, script, view) in self.tables
        ]


class FillTableFromView(DBAccessTask, I2B2Task):
    table = StrParam(description='PCORNet CDM table name')
    script = cast(Script, luigi.EnumParameter(
        enum=Script, description='script to build view'))
    view = StrParam(description='Transformation view')
    parallel_degree = IntParam(default=12)

    complete_test = 'select refresh_{table}_date from harvest'

    def requires(self):
        return [
            self.project,  # I2B2 project
            SqlScriptTask(script=self.script,
                          param_vars=self.variables),
            SqlScriptTask(script=Script.cdm_harvest_init,
                          param_vars=self.variables)
        ]

    @property
    def variables(self) -> Environment:
        return dict(I2B2STAR=self.project.star_schema)

    def complete(self):
        deps = luigi.task.flatten(self.requires())
        if not all(t.complete() for t in deps):
            return False

        table = self.table
        with self.connection('{0} fresh?'.format(table)) as work:
            refreshed_at = work.scalar(self.complete_test.format(table=table))
        return refreshed_at is not None

    steps = [
        'truncate table {table}',
        'insert /*+ parallel({parallel_degree}) append */ into {table} select * from {view}',
        "update harvest set refresh_{table}_date = sysdate, datamart_claims = (select present from harvest_enum)"
    ]

    def run(self):
        with self.connection('refresh {table}'.format(table=self.table)) as work:
            for step in self.steps:
                work.execute(step.format(table=self.table, view=self.view,
                                         parallel_degree=self.parallel_degree))


class HarvestInit(SqlScriptTask):
    script = Script.cdm_harvest_init
