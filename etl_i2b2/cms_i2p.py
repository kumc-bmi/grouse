'''cms_i2p -- i2b2 to PCORNet CDM optimized for CMS

The `I2P` task plays the same role, architecturally, as
`i2p-transform`__; that is: it builds a PCORNet CDM datamart from an
I2B2 datamart. But where `i2p-transform`__ uses a pattern of `insert
... select distinct ... observation_fact x /*self/* join
observation_fact y` to accomodate a wide variety of ways of organizing
data in i2b2, we can avoid much of the aggregation and self-joins
because our I2B2 datamart was built with transformation to PCORNet CDM
in mind; for example: dianosis facts are 1-1 between i2b2
`observation_fact` and PCORNet `DIAGNOSIS` and the `instance_num`
provides a primary key (within the scope of a `patient_num`).


__ https://github.com/kumc-bmi/i2p-transform

'''

from typing import List, cast

import luigi

from etl_tasks import DBAccessTask, I2B2Task, SqlScriptTask
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Environment


class I2P(luigi.WrapperTask):
    '''Transform I2B2 datamart to PCORNet CDM datamart.
    '''

    "Read (T, S, V) as: to build T, ensure S has been run and insert from V."
    tables = [
        ('DEMOGRAPHIC', Script.cms_dem_dstats, 'pcornet_demographic'),
        ('ENCOUNTER', Script.cms_enc_dstats, 'pcornet_encounter'),
        ('DIAGNOSIS', Script.cms_dx_dstats, 'pcornet_diagnosis'),
    ]

    def requires(self) -> List[luigi.Task]:
        return [
            FillTableFromView(table=table, script=script, view=view)
            for (table, script, view) in self.tables
        ]


class HarvestInit(SqlScriptTask):
    '''Create HARVEST table with one row.
    '''
    script = Script.cdm_harvest_init
    schema = StrParam(description='PCORNet CDM schema name',
                      default='CMS_PCORNET_CDM')

    @property
    def variables(self) -> Environment:
        return dict(PCORNET_CDM=self.schema)


class FillTableFromView(DBAccessTask, I2B2Task):
    '''Fill (insert into) PCORNet CDM table from a view of I2B2 data.

    Use HARVEST refresh columns to track completion status.
    '''
    # TODO: consider an enumeration of CDM table names.
    table = StrParam(description='PCORNet CDM table name')
    script = cast(Script, luigi.EnumParameter(
        enum=Script, description='script to build view'))
    view = StrParam(description='Transformation view')
    parallel_degree = IntParam(default=12)

    complete_test = 'select refresh_{table}_date from {ps}.harvest'

    @property
    def harvest(self) -> HarvestInit:
        return HarvestInit()

    def requires(self) -> List[luigi.Task]:
        return [
            self.project,  # I2B2 project
            SqlScriptTask(script=self.script,
                          param_vars=self.variables),
            SqlScriptTask(script=Script.cdm_harvest_init,
                          param_vars=self.variables)
        ]

    @property
    def variables(self) -> Environment:
        return dict(I2B2STAR=self.project.star_schema,
                    PCORNET_CDM=self.harvest.schema)

    def complete(self) -> bool:
        deps = luigi.task.flatten(self.requires())  # type: List[luigi.Task]
        if not all(t.complete() for t in deps):
            return False

        table = self.table
        schema = self.harvest.schema
        with self.connection('{0} fresh?'.format(table)) as work:
            refreshed_at = work.scalar(self.complete_test.format(
                ps=schema, table=table))
        return refreshed_at is not None

    steps = [
        'delete from {ps}.{table}',  # ISSUE: lack of truncate privilege is a pain.
        'commit',
        'insert /*+ parallel({parallel_degree}) append */ into {ps}.{table} select * from {view}',
        "update {ps}.harvest set refresh_{table}_date = sysdate, datamart_claims = (select present from harvest_enum)"
    ]

    def run(self) -> None:
        with self.connection('refresh {table}'.format(table=self.table)) as work:
            for step in self.steps:
                work.execute(step.format(table=self.table, view=self.view,
                                         ps=self.harvest.schema,
                                         parallel_degree=self.parallel_degree))
