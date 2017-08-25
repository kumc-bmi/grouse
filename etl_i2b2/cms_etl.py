'''cms_etl -- Load an i2b2 star schema from CMS RIF data

See README.md background and usage.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

from abc import abstractproperty
from datetime import datetime
from typing import Iterable, List, cast
import logging

from luigi.parameter import FrozenOrderedDict
from sqlalchemy import MetaData
from sqlalchemy.engine import RowProxy
from sqlalchemy.exc import DatabaseError
import luigi

from etl_tasks import (
    SqlScriptTask, UploadTask, ReportTask, SourceTask,
    DBAccessTask, DBTarget, LoggedConnection, SchemaTarget,
    I2B2ProjectCreate, I2B2Task,
    TimeStampParameter)
from param_val import StrParam, IntParam
import param_val as pv
from script_lib import Script
import script_lib as lib
from sql_syntax import Environment, ViewId, Params

log = logging.getLogger(__name__)
TimeStampParam = pv._valueOf(datetime(2001, 1, 1, 0, 0, 0), TimeStampParameter)


class CMSExtract(SourceTask, DBAccessTask):
    download_date = TimeStampParam(description='see luigi.cfg.example')
    cms_rif = StrParam(description='see luigi.cfg.example')
    bene_chunks = IntParam(default=64, significant=False,
                           description='see luigi.cfg.example')

    script_variable = 'cms_source_cd'
    source_cd = "'ccwdata.org'"

    table_eg = 'mbsf_ab_summary'

    def _dbtarget(self) -> DBTarget:
        return SchemaTarget(self._make_url(self.account),
                            schema_name=self.cms_rif,
                            table_eg=self.table_eg,
                            echo=self.echo)

    def table_details(self, lc: LoggedConnection, tables: List[str]) -> MetaData:
        rif_meta = MetaData(schema=self.cms_rif)
        rif_meta.reflect(only=tables, schema=self.cms_rif,
                         bind=self._dbtarget().engine)
        return rif_meta


class GrouseETL(luigi.WrapperTask):
    def reports(self) -> List[luigi.Task]:
        return [
            Demographics(),
            Encounters(),
            Diagnoses(),
        ]

    def requires(self) -> List[luigi.Task]:
        return self.reports()


class GrouseRollback(DBAccessTask):
    def complete(self) -> bool:
        return False

    def requires(self) -> List[luigi.Task]:
        return []

    def run(self) -> None:
        top = GrouseETL()
        for report in top.reports():
            out = report.output()
            if out.exists():
                log.info('removing %s', out.fn)
                out.remove()

        script_tasks = [
            st
            for st in _deep_requires(top)
            if isinstance(st, SqlScriptTask)]
        tables = frozenset(
            table_name
            for stask in script_tasks
            for dep in stask.script.dep_closure()
            for table_name in dep.inserted_tables(stask.variables))
        objects = frozenset(
            obj
            for stask in script_tasks
            for dep in stask.script.dep_closure()
            for obj in dep.created_objects())

        with self.connection() as work:
            work.log.info('resetting load_status for all uploads')
            done = work.execute(
                '''
                update {i2b2}.upload_status
                set load_status = null
                where load_status is not null
                '''.format(i2b2=I2B2ProjectCreate().star_schema))
            log.info('%(upload_count)d uploads reset', dict(upload_count=done.rowcount))

            for table_name in tables:
                try:
                    work.execute('truncate table {t}'.format(t=table_name))
                except DatabaseError:
                    pass

            for oid in objects:
                try:
                    work.execute('drop {object}'.format(object=oid))
                except DatabaseError:
                    pass


def _deep_requires(t: luigi.Task) -> Iterable[luigi.Task]:
    yield t
    for child in luigi.task.flatten(t.requires()):
        for anc in _deep_requires(cast(luigi.Task, child)):
            yield anc


def _canonical_params(t: luigi.Task) -> FrozenOrderedDict:
    return FrozenOrderedDict(sorted(t.to_str_params(only_significant=True).items()))


class FromCMS(I2B2Task):
    '''Mix in source and substitution variables for CMS ETL scripts.

    The signature of such tasks should depend on all and only the significant
    parameters of the CMSExtract; for example, if we reload the data or
    break it into a different number of chunks, the signature should change.
    '''
    # ISSUE: ambient. Relies on client.cfg for unit testing.
    source_params = pv.DictParam(default=_canonical_params(CMSExtract()))

    @property
    def source(self) -> CMSExtract:
        return CMSExtract.from_str_params(self.source_params)

    @property
    def variables(self) -> Environment:
        return self.vars_for_deps

    @property
    def vars_for_deps(self) -> Environment:
        config = [(lib.I2B2STAR, self.project.star_schema),
                  (lib.CMS_RIF, self.source.cms_rif)]
        design = [(CMSExtract.script_variable, CMSExtract.source_cd)]
        return dict(config + design)


class _MappingTask(FromCMS, UploadTask):
    def requires(self) -> List[luigi.Task]:
        reset = MappingReset()
        return UploadTask.requires(self) + [self.source, reset]

    @property
    def variables(self) -> Environment:
        return self.vars_for_deps


class _DimensionTask(FromCMS, UploadTask):
    @property
    def mappings(self) -> List[luigi.Task]:
        return []

    def requires(self) -> List[luigi.Task]:
        return SqlScriptTask.requires(self) + self.mappings


class _FactLoadTask(FromCMS, UploadTask):
    script = Script.cms_facts_load

    @abstractproperty
    def txform(self) -> Script:
        raise NotImplemented

    @abstractproperty
    def fact_view(self) -> str:
        raise NotImplemented

    @abstractproperty
    def mappings(self) -> List[luigi.Task]:
        raise NotImplemented

    @property
    def label(self) -> str:
        return self.txform.title

    @property
    def variables(self) -> Environment:
        assert ViewId(self.fact_view) in self.txform.created_objects()
        return dict(self.vars_for_deps,
                    fact_view=self.fact_view)

    def requires(self) -> List[luigi.Task]:
        txform = SqlScriptTask(
            script=self.txform,
            param_vars=self.vars_for_deps)
        return (self.mappings +
                SqlScriptTask.requires(self) +
                [txform] + SqlScriptTask.requires(txform))


class FactLoadRepair(FromCMS, UploadTask):
    '''Finish up if building observation_fact_nnn succeeds but moving the
    data to observation_fact fails.
    '''
    upload_id = IntParam()
    statement = '''
        with io as (
          select clock_access('bulk load clock') clock
          from dual
        )
        select p.*
        from io, table(obs_load_repair(
            clock => io.clock,
            upload_id => :upload_id)) p
        '''
    # TODO: 'truncate table %(fact_part)s'
    script = Script.cms_facts_load  # required by UploadTask but not used
    lib = Script.obs_fact_pipe

    def requires(self) -> List[luigi.Task]:
        return [self.project, self.source, SqlScriptTask(self.lib)]

    def run(self) -> None:
        upload = self._upload_target()
        with upload.job(self, upload_id=self.upload_id) as conn_id_r:
            conn, upload_id, result = conn_id_r
            fact_part = 'observation_fact_%d' % upload_id  # KLUDGE: assume I2B2STAR
            conn.log.info('%(event)s %(fact_part)s...',
                          dict(event='repair', fact_part=fact_part))
            event = conn.execute(self.statement, dict(upload_id=upload_id)).fetchone()
            bulk_rows = event.row_count
            conn.log.info('%(event)s %(fact_part)s: %(bulk_rows)d rows',
                          dict(event='repair', fact_part=fact_part, bulk_rows=bulk_rows))
            result[upload.table.c.loaded_record.name] = bulk_rows


class MedparFactGroupLoad(_FactLoadTask, FromCMS):
    '''A group of facts that roll-up encounters by MEDPAR.

    See pat_day_medpar_rollup() in cms_keys.pls
    '''
    fact_view = StrParam()
    txform = cast(Script, luigi.EnumParameter(enum=Script))

    @property
    def mappings(self) -> List[luigi.Task]:
        return [PatientMapping(),
                MedparMapping()]


class DemographicFactsLoad(luigi.WrapperTask):
    txform = Script.mbsf_pivot
    fact_views = ['cms_mbsf_facts',
                  'cms_maxdata_ps_facts']

    def requires(self) -> List[luigi.Task]:
        return [
            MedparFactGroupLoad(txform=self.txform,
                                fact_view=view)
            for view in self.fact_views
        ]


class MedparLoad(luigi.WrapperTask):
    fact_views = [
        'cms_medpar_dx', 'cms_medpar_px', 'cms_medpar_facts'
    ]
    txform = Script.medpar_pivot

    def requires(self) -> List[luigi.Task]:
        return [MedparFactGroupLoad(fact_view=fv,
                                    txform=self.txform)
                for fv in self.fact_views]


class BeneIdSurvey(FromCMS, SqlScriptTask):
    source_table = StrParam()
    script = Script.bene_chunks_survey

    @property
    def variables(self) -> Environment:
        config = [(lib.CMS_RIF, self.source.cms_rif)]
        return dict(config,
                    source_table=self.source_table.upper(),
                    chunk_qty=str(self.source.bene_chunks))

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(
            chunk_qty=self.source.bene_chunks))

    def results(self) -> List[RowProxy]:
        with self.connection(event='survey results') as lc:
            q = '''
              select chunk_num
                , chunk_rows
                , bene_id_first
                , bene_id_last
              from bene_chunks
              where bene_id_source = :source_table
                and chunk_qty = :chunk_qty
              order by chunk_num
            '''
            params = dict(source_table=self.source_table.upper(),
                          chunk_qty=self.source.bene_chunks)  # type: Params
            Params  # tell flake8 we're using it.
            try:
                return lc.execute(q, params=params).fetchall()
            except DatabaseError:
                return []


class PatientMapping(FromCMS, SqlScriptTask):
    '''Ensure patient mappings were generated.
    See ../deid for details.
    '''
    script = Script.cms_patient_mapping


class PatientDimension(_DimensionTask, FromCMS):
    script = Script.cms_patient_dimension

    @property
    def mappings(self) -> List[luigi.Task]:
        return [PatientMapping()]


class MappingReset(FromCMS, UploadTask):
    script = Script.mapping_reset


class Demographics(ReportTask):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self) -> List[luigi.Task]:
        raise NotImplementedError
        pd = PatientDimension()
        report = SqlScriptTask(script=self.script,
                               param_vars=pd.vars_for_deps)  # type: luigi.Task
        return [report] + cast(List[luigi.Task], [pd])


class MedparMapping(_MappingTask, FromCMS):
    script = Script.medpar_encounter_map
    resources = {'encounter_mapping': 1}


class VisitDimension(_DimensionTask):
    script = Script.cms_visit_dimension

    @property
    def mappings(self) -> List[luigi.Task]:
        # return [BeneGroupMapping("@@"), MedParMapping()]
        raise NotImplementedError


class Encounters(ReportTask):
    script = Script.cms_enc_dstats
    report_name = 'encounters_per_visit_patient'

    @property
    def data_task(self) -> luigi.Task:
        return VisitDimension()


class PatientDayDxGroupLoad(luigi.WrapperTask):
    group_num = IntParam()
    fact_views = [
        'cms_bcarrier_dx', 'cms_bcarrier_line_dx', 'cms_outpatient_claims_dx'
    ]
    txform = Script.cms_dx_txform

    def requires(self) -> List[luigi.Task]:
        raise NotImplementedError


class Diagnoses(FromCMS, ReportTask):
    script = Script.cms_dx_dstats
    report_name = 'dx_by_enc_type'

    def requires(self) -> List[luigi.Task]:
        raise NotImplementedError


if __name__ == '__main__':
    luigi.build([GrouseETL()], local_scheduler=True)
