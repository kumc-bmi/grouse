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
from sqlalchemy.exc import DatabaseError
import luigi

from etl_tasks import (
    LoggedConnection,
    SqlScriptTask, UploadTask, ReportTask, SourceTask,
    DBAccessTask, I2B2ProjectCreate, I2B2Task,
    TimeStampParameter)
from param_val import StrParam, IntParam
import param_val as pv
from script_lib import Script, ChunkByBene
import script_lib as lib
from sql_syntax import Name, Environment, Params, ViewId

log = logging.getLogger(__name__)
TimeStampParam = pv._valueOf(datetime(2001, 1, 1, 0, 0, 0), TimeStampParameter)


class CMSExtract(SourceTask):
    download_date = TimeStampParam(description='see luigi.cfg.example')
    cms_rif = StrParam(description='see luigi.cfg.example')
    bene_chunks = IntParam(default=1,
                           description='see luigi.cfg.example')
    group_qty = IntParam(default=1,
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
    # ISSUE: ambient. interferes with unit testing.
    source_params = pv.DictParam(default=_canonical_params(CMSExtract()))
    # for testing, replace the above line with:
    # source_params = pv.DictParam()

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
        survey = BeneIdSurvey()
        return UploadTask.requires(self) + [self.source, survey, reset]

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
    group_num = IntParam()

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
            param_vars=self.vars_for_deps)  # type: luigi.Task
        return SqlScriptTask.requires(self) + self.mappings + [txform]


class _BeneChunked(FromCMS, DBAccessTask):
    group_num = IntParam()

    def chunks(self, conn: LoggedConnection, names_present: List[Name]) -> List[Params]:
        if not ChunkByBene.required_params <= set(names_present):
            return [{}]

        qty = self.source.bene_chunks
        first, last = ChunkByBene.group_chunks(
            qty, self.source.group_qty, self.group_num)
        with conn.log.step('%(event)s', dict(event='find chunks')) as step:
            result = conn.execute(ChunkByBene.lookup_sql,
                                  dict(qty=qty, first=first, last=last)).fetchall()
            bounds, sizes = ChunkByBene.result_chunks(result)
            step.msg_parts.append(' %(first)d thru %(last)d sizes: %(sizes)s...')
            step.argobj.update(dict(first=first, last=last, sizes=sizes[:3]))
        return bounds


class MedparFactGroupLoad(_BeneChunked, _FactLoadTask):
    '''A group of facts that roll-up encounters by MEDPAR.

    See pat_day_medpar_rollup() in cms_keys.pls
    '''
    fact_view = StrParam()
    txform = cast(Script, luigi.EnumParameter(enum=Script))

    @property
    def mappings(self) -> List[luigi.Task]:
        return [PatientMapping(),
                MedparMapping(group_num=self.group_num)]


class BeneficiarySummaryGroupLoad(MedparFactGroupLoad):
    txform = Script.mbsf_pivot
    fact_view = 'cms_mbsf_facts'


class BeneficiarySummaryLoad(luigi.WrapperTask):
    def requires(self) -> List[luigi.Task]:
        group_qty = CMSExtract().group_qty
        assert group_qty > 0, 'TODO: PosIntParamter'
        return [
            BeneficiarySummaryGroupLoad(group_num=num)
            for num in range(1, group_qty + 1)]


class MedparGroupLoad(luigi.WrapperTask):
    group_num = IntParam()
    fact_views = [
        'cms_medpar_dx', 'cms_medpar_px', 'cms_medpar_facts'
    ]
    txform = Script.medpar_pivot

    def requires(self) -> List[luigi.Task]:
        return [MedparFactGroupLoad(group_num=self.group_num,
                                    fact_view=fv,
                                    txform=self.txform)
                for fv in self.fact_views]


class MedparLoad(luigi.WrapperTask):
    def requires(self) -> List[luigi.Task]:
        group_qty = CMSExtract().group_qty
        assert group_qty > 0, 'TODO: PosIntParamter'
        return [
            MedparGroupLoad(group_num=num)
            for num in range(1, group_qty + 1)]


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


class PatientMapping(FromCMS, SqlScriptTask):
    '''Ensure patient mappings were generated.
    See ../deid for details.
    '''
    script = Script.cms_patient_mapping


class PatientDimensionGroup(_BeneChunked, _DimensionTask):
    group_num = IntParam()
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
        group_qty = CMSExtract().group_qty
        assert group_qty > 0, 'TODO: PosIntParamter'
        groups = [
            PatientDimensionGroup(group_num=num)
            for num in range(1, group_qty + 1)]
        report = SqlScriptTask(script=self.script,
                               param_vars=groups[0].vars_for_deps)  # type: luigi.Task
        return [report] + cast(List[luigi.Task], groups)


class MedparMapping(_BeneChunked, _MappingTask):
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
        return [PatientDayFactGroupLoad(group_num=self.group_num,
                                        fact_view=fv,
                                        txform=self.txform)
                for fv in self.fact_views]


class Diagnoses(FromCMS, ReportTask):
    script = Script.cms_dx_dstats
    report_name = 'dx_by_enc_type'

    def requires(self) -> List[luigi.Task]:
        return [
            task_class(group_num=g)
            for task_class in [MedparDxGroupLoad, PatientDayDxGroupLoad]
            for g in range(1, self.source.group_qty + 1)]


if __name__ == '__main__':
    luigi.build([GrouseETL()], local_scheduler=True)
