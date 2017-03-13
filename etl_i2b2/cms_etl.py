'''cms_etl -- Load an i2b2 star schema from CMS RIF data

See README.md background and usage.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

from abc import abstractproperty
from datetime import datetime
from typing import Iterable, List, cast
import logging

from sqlalchemy.exc import DatabaseError
import luigi

from etl_tasks import (
    SqlScriptTask, UploadTask, ReportTask, SourceTask,
    DBAccessTask, I2B2ProjectCreate, I2B2Task,
    TimeStampParameter)
from param_val import StrParam, IntParam
from script_lib import Script, ChunkByBene
import script_lib as lib
from sql_syntax import Name, Environment, Params

log = logging.getLogger(__name__)


class CMSExtract(SourceTask):
    download_date = cast(datetime, TimeStampParameter(description='see luigi.cfg.example'))
    cms_rif = StrParam(description='see luigi.cfg.example')
    bene_chunks = IntParam(default=1,
                           description='see luigi.cfg.example')
    script_variable = 'cms_source_cd'
    source_cd = "'ccwdata.org'"

    def complete(self) -> bool:
        return bool(self.download_date)


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

        with self.dbtrx() as work:
            log.info('resetting load_status for all uploads')
            done = work.execute(
                '''
                update {i2b2}.upload_status
                set load_status = null
                where load_status is not null
                '''.format(i2b2=I2B2ProjectCreate().star_schema))
            log.info('%d uploads reset', done.rowcount)

            for table_name in tables:
                try:
                    work.execute('truncate table {t}'.format(t=table_name))
                    log.info('truncated %s', table_name)
                except DatabaseError:
                    pass

            for oid in objects:
                try:
                    work.execute('drop {object}'.format(object=oid))
                    log.info('dropped %s', oid)
                except DatabaseError:
                    pass


def _deep_requires(t: luigi.Task) -> Iterable[luigi.Task]:
    yield t
    for child in luigi.task.flatten(t.requires()):
        for anc in _deep_requires(cast(luigi.Task, child)):
            yield anc


class FromCMS(I2B2Task):
    '''Mix in source and substitution variables for CMS ETL scripts.
    '''

    @property
    def source(self) -> CMSExtract:
        return CMSExtract()

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
        return UploadTask.requires(self) + [self.source]


class _DimensionTask(FromCMS, UploadTask):
    def mappings(self) -> List[luigi.Task]:
        return []

    def requires(self) -> List[luigi.Task]:
        return SqlScriptTask.requires(self) + self.mappings()


class _FactLoadTask(FromCMS, UploadTask):
    script = Script.cms_facts_load

    @abstractproperty
    def txform(self) -> Script:
        raise NotImplemented

    @abstractproperty
    def fact_view(self) -> str:
        raise NotImplemented

    @property
    def label(self) -> str:
        return self.txform.title

    @property
    def variables(self) -> Environment:
        return dict(self.vars_for_deps,
                    fact_view=self.fact_view)

    def requires(self) -> List[luigi.Task]:
        mappings = [_BeneGroupSourceMapping(), EncounterMapping()]  # type: List[luigi.Task]
        txform = SqlScriptTask(
            script=self.txform,
            param_vars=self.vars_for_deps)  # type: luigi.Task
        return SqlScriptTask.requires(self) + mappings + [txform]


class _BeneChunked(FromCMS, DBAccessTask):
    group_qty = IntParam()
    group_num = IntParam()

    def chunks(self, names_present: List[Name]) -> List[Params]:
        if not ChunkByBene.required_params <= set(names_present):
            return [{}]

        qty = self.source.bene_chunks
        first, last = ChunkByBene.group_chunks(
            qty, self.group_qty, self.group_num)
        with self.dbtrx() as q:
            result = q.execute(ChunkByBene.lookup_sql,
                               qty=qty, first=first, last=last).fetchall()
            bounds, sizes = ChunkByBene.result_chunks(result)
            log.info('chunks: %d thru %d sizes: %s...',
                     first, last, sizes[:3])
        return bounds


class BeneIdSurvey(FromCMS, SqlScriptTask):
    script = Script.bene_chunks_survey

    @property
    def variables(self) -> Environment:
        return dict(
            self.vars_for_deps,
            chunk_qty=str(self.source.bene_chunks))

    @property
    def vars_for_deps(self) -> Environment:
        config = [(lib.CMS_RIF, self.source.cms_rif)]
        return dict(config,
                    chunk_qty=str(self.source.bene_chunks))

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(
            chunk_qty=self.source.bene_chunks))


class _BeneGroupSourceMapping(_BeneChunked, UploadTask):
    '''Patient mapping for one group from one source table.
    '''
    script = Script.cms_patient_mapping

    def requires(self) -> List[luigi.Task]:
        survey = BeneIdSurvey()
        return UploadTask.requires(self) + [survey, self.source]

    @property
    def variables(self) -> Environment:
        return self.vars_for_deps


class PatientDimensionGroup(_BeneChunked, _DimensionTask):
    # TODO: _BeneChunked
    group_qty = IntParam()
    group_num = IntParam()
    script = Script.cms_patient_dimension

    def mappings(self) -> List[luigi.Task]:
        return [_BeneGroupSourceMapping(group_qty=self.group_qty,
                                        group_num=self.group_num)]


class Demographics(ReportTask):
    group_qty = IntParam(default=1)
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self) -> List[luigi.Task]:
        assert self.group_qty > 0, 'TODO: PosIntParamter'
        [src] = ChunkByBene.sources_from(PatientDimensionGroup.script)
        groups = [
            PatientDimensionGroup(group_qty=self.group_qty,
                                  group_num=num)
            for num in range(1, self.group_qty + 1)]
        report = SqlScriptTask(script=self.script,
                               param_vars=groups[0].vars_for_deps)  # type: luigi.Task
        return [report] + cast(List[luigi.Task], groups)


class EncounterMapping(_BeneChunked, _MappingTask):
    script = Script.cms_encounter_mapping


class VisitDimension(_DimensionTask):
    script = Script.cms_visit_dimension

    def mappings(self) -> List[luigi.Task]:
        return [_BeneGroupSourceMapping(), EncounterMapping()]


class Encounters(ReportTask):
    script = Script.cms_enc_dstats
    report_name = 'encounters_per_visit_patient'

    @property
    def data_task(self) -> luigi.Task:
        return VisitDimension()


class DiagnosesLoad(_BeneChunked, _FactLoadTask):
    fact_view = 'observation_fact_cms_dx'
    txform = Script.cms_dx_txform

    @property
    def chunk_source(self) -> str:
        return '''
        (select distinct bene_id from {cms_rif}.bcarrier_claims)
        '''.format(cms_rif=self.source.cms_rif)


class Diagnoses(ReportTask):
    script = Script.cms_dx_dstats
    report_name = 'dx_by_enc_type'

    @property
    def data_task(self) -> luigi.Task:
        return DiagnosesLoad()


if __name__ == '__main__':
    luigi.build([GrouseETL()], local_scheduler=True)
