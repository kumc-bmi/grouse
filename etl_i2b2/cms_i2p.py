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

from typing import List, Tuple, cast

import luigi
import pandas as pd  # type: ignore

from cms_pd import read_sql_step, obj_string
from etl_tasks import DBAccessTask, I2B2Task, SqlScriptTask, LoggedConnection, log_plan
from eventlog import EventLogger
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Environment, Params


class I2P(luigi.WrapperTask):
    '''Transform I2B2 datamart to PCORNet CDM datamart.
    '''

    def requires(self) -> List[luigi.Task]:
        return [
            FillTableFromView(table='DEMOGRAPHIC', script=Script.cms_dem_dstats, view='pcornet_demographic'),
            FillTableFromView(table='ENCOUNTER', script=Script.cms_enc_dstats, view='pcornet_encounter'),
            ProceduresLoad()
            # TODO: ('DIAGNOSIS', Script.cms_dx_dstats, None, None),
            # TODO: ('DISPENSING', Script.cms_drug_dstats, 'pcornet_dispensing'),

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
           select * from {view} where patid between :lo and :hi'''

    def load(self, work: LoggedConnection) -> None:
        groups = self.project.patient_groups(work, self.pat_group_qty)
        step = self.bulk_insert.format(table=self.table, view=self.view,
                                       ps=self.harvest.schema,
                                       degree=self.parallel_degree)
        log_plan(work, 'fill chunk of {table}'.format(table=self.table), {},
                 sql=step)
        for (qty, num, lo, hi) in groups:
            work.execute(step, params=dict(lo=lo, hi=hi))


class ProceduresLoad(_HarvestRefresh):
    table = StrParam(default='PROCEDURES')
    pat_group_qty = IntParam(default=100, significant=False,
                             description='bound enc_type mapping chunks too')
    chunksize = IntParam(default=50000, significant=False)
    parallel_degree = IntParam(default=12, significant=False)

    obs_q = """
        select /*+ parallel({degree}) */
               upload_id, patient_num, instance_num
             , encounter_num, start_date, concept_cd
        from {i2b2_star}.observation_fact obs
        where patient_num between :lo and :hi
          and regexp_like(obs.concept_cd, :proc_pat)
        """
    proc_pat = r'(^(CPT|HCPCS|ICD10PCS):)|(^ICD9:\d{2}\.\d{1,2})'

    def requires(self) -> List[luigi.Task]:
        return [
            self.project,  # I2B2 project
            self.harvest,
            SqlScriptTask(script=Script.cms_dx_dstats,
                          param_vars=self.variables),
        ]

    def load(self, lc: LoggedConnection) -> None:
        def merge(xwalk: pd.DataFrame, key: pd.Series) -> pd.DataFrame:
            assert not any(xwalk.index.duplicated())
            oob = ~key.isin(xwalk.index)
            if any(oob):
                lc.log.warning('%d %s (%0.1f%% of %d: %s...) out of bounds of %s',  # ISSUE
                               len(key[oob]), key.name,
                               100.0 * len(key[oob]) / len(key), len(key), key[:3].values,
                               xwalk.columns.values)
                key = key[~oob]
            vals = xwalk.loc[key]
            vals.index = key.index
            return vals

        px_meta = self.proc_code_map(lc)
        pat_groups = self.project.patient_groups(lc, self.pat_group_qty)
        target = '{ps}.{table}'.format(ps=self.harvest.schema, table=self.table)
        proceduresid = 0

        for _, _, lo, hi in pat_groups:
            enc_type_map = self.enc_type_group(lc, (lo, hi))
            for obs in pd.read_sql(self.obs_q
                                   .format(i2b2_star=self.project.star_schema, degree=self.parallel_degree),
                                   params=dict(proc_pat=self.proc_pat, lo=lo, hi=hi),
                                   con=lc._conn,
                                   chunksize=self.chunksize):
                with lc.log.step('%(event)s into %(target)s %(obs_qty)d obs',
                                 dict(event='insert observations', target=target, obs_qty=len(obs))) as step:
                    px = merge(px_meta, key=obs.concept_cd)
                    obs = obs[obs.concept_cd.isin(px_meta.index)]     # ISSUE: drop facts with no px_meta
                    enc = merge(enc_type_map, key=obs.encounter_num)  # ISSUE: obs with no visit_dimension get null.
                    proc = pd.DataFrame(dict(
                        patid=obs.patient_num,
                        encounterid=obs.encounter_num,
                        enc_type=enc.enc_type,
                        admit_date=enc.admit_date,
                        providerid=enc.providerid,
                        px_date=obs.start_date,
                        px=px.px,
                        px_type=px.px_type,
                        px_source='CL',
                        raw_px=obs.instance_num,
                        raw_px_type=obs.upload_id),
                    )
                    proc.index = pd.RangeIndex(proceduresid, proceduresid + len(proc))
                    proc.index.names = ['proceduresid']

                    assert not any(proc.index.duplicated())

                    proc.to_sql(schema=self.harvest.schema, name=self.table,
                                con=lc._conn, dtype=obj_string(proc), if_exists='append')
                    step.argobj.update(dict(proc_ix_start=proceduresid, proc_qty=len(proc)))
                    step.msg_parts.append(' proceduresid %(proc_ix_start)d + %(proc_qty)d')
                    proceduresid += len(proc)

    @classmethod
    def proc_code_map(cls, lc: LoggedConnection) -> pd.DataFrame:
        xwalk = read_sql_step("""select * from px_meta""", lc).set_index('concept_cd')
        lc.log.info('px_meta rows: %d', len(xwalk))
        oops = xwalk[~xwalk.index.str.match(cls.proc_pat, as_indexer=True)]
        if len(oops) > 0:
            raise ValueError(oops, 'bug in px_meta view (from cms_dx_dstats.sql)')
        return xwalk

    def enc_type_group(self, lc: LoggedConnection, pat_range: Tuple[int, int]) -> pd.DataFrame:
        lo, hi = pat_range
        params = cast(Params, dict(lo=lo, hi=hi))
        sql = """
            select /*+ parallel({degree}) */ encounter_num
                 , inout_cd enc_type, start_date admit_date, providerid
            from {i2b2_star}.visit_dimension
            where patient_num between :lo and :hi
            """.format(i2b2_star=self.project.star_schema, degree=self.parallel_degree)
        log_plan(lc, 'visit_dimension for pat group', params=params, sql=sql)
        enc = read_sql_step(sql, lc,
                            params=params, show_lines=2).set_index('encounter_num')
        return _clean_dups(enc, lc.log)  # ISSUE: I found 3 IP encounters with duplicate encounter_num.


def _clean_dups(df: pd.DataFrame, log: EventLogger) -> pd.DataFrame:
    dup_ix = df.index.duplicated()
    if any(dup_ix):
        log.warning('%d dups; e.g.:\n%s', len(df[dup_ix]), df[dup_ix].head(40))
        df = df[~dup_ix]
    return df
