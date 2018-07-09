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

from etl_tasks import DBAccessTask, I2B2Task, SqlScriptTask, LoggedConnection, log_plan
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Environment


class I2P(luigi.WrapperTask):
    '''Transform I2B2 datamart to PCORNet CDM datamart.
    '''

    def requires(self) -> List[luigi.Task]:
        return [
            FillTableFromView(table='DEMOGRAPHIC', script=Script.cms_dem_dstats, view='pcornet_demographic'),
            FillTableFromView(table='ENCOUNTER', script=Script.cms_enc_dstats, view='pcornet_encounter'),
            FillTableFromView(table='DIAGNOSIS', script=Script.cms_dx_dstats, view='pcornet_diagnosis'),
            FillTableFromView(table='PROCEDURES', script=Script.cms_dx_dstats, view='pcornet_procedures'),
            FillTableFromView(table='DISPENSING', script=Script.cms_drug_dstats, view='pcornet_dispensing'),

            # TODO: ENROLLMENT
            # N/A: VITAL
            # N/A: LAB_RESULT_CM
            # N/A: PRO_CM
            # N/A: PRESCRIBING
            # N/A: PCORNET_TRIAL
            # TODO: DEATH
            # TODO: DEATH_CAUSE
        ]


class HarvestInit(SqlScriptTask):
    '''Create HARVEST table with one row.
    '''
    script = Script.cdm_harvest_init
    schema = StrParam(description='PCORNet CDM schema name',
                      default='CMS_PCORNET_CDM')

    @classmethod
    def script_with_vars(self, variables: Environment) -> SqlScriptTask:
        return SqlScriptTask(script=self.script,
                             param_vars=variables)

    @property
    def variables(self) -> Environment:
        return dict(PCORNET_CDM=self.schema)


class _HarvestRefresh(DBAccessTask, I2B2Task):
    table = StrParam(description='PCORNet CDM data table name')

    # The PCORNet CDM HARVEST table has a refresh column for each
    # of the data tables -- 14 of them as of version 3.1.
    complete_test = 'select refresh_{table}_date from {ps}.harvest'

    @property
    def harvest(self) -> HarvestInit:
        return HarvestInit()

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

    @property
    def variables(self) -> Environment:
        return dict(I2B2STAR=self.project.star_schema,
                    PCORNET_CDM=self.harvest.schema)

    steps = [
        'delete from {ps}.{table}',  # ISSUE: lack of truncate privilege is a pain.
        'commit',
        None,
        "update {ps}.harvest set refresh_{table}_date = sysdate, datamart_claims = (select present from harvest_enum)"
    ]

    def run(self) -> None:
        with self.connection('refresh {table}'.format(table=self.table)) as work:
            for step in self.steps:
                if step is None:
                    self.load(work)
                else:
                    step = step.format(table=self.table, ps=self.harvest.schema)
                    work.execute(step)

    def load(self, work: LoggedConnection) -> None:
        raise NotImplementedError('abstract')


class FillTableFromView(_HarvestRefresh):
    '''Fill (insert into) PCORNet CDM table from a view of I2B2 data.

    Use HARVEST refresh columns to track completion status.
    '''
    script = cast(Script, luigi.EnumParameter(
        enum=Script, description='script to build view'))
    view = StrParam(description='Transformation view')
    parallel_degree = IntParam(default=6, significant=False)
    pat_group_qty = IntParam(default=6, significant=False)

    def requires(self) -> List[luigi.Task]:
        return [
            self.project,  # I2B2 project
            SqlScriptTask(script=self.script,
                          param_vars=self.variables),
            HarvestInit.script_with_vars(self.variables)
        ]

    bulk_insert = '''insert /*+ append parallel({degree}) */ into {ps}.{table}
           select * from {view}'''

    def load(self, work: LoggedConnection) -> None:
        step = self.bulk_insert.format(table=self.table, view=self.view,
                                       ps=self.harvest.schema,
                                       degree=self.parallel_degree)
        log_plan(work, 'fill chunk of {table}'.format(table=self.table), {},
                 sql=step)
        work.execute(step)
