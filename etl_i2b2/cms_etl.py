'''cms_etl -- Load an i2b2 star schema from CMS RIF data

See README.md background and usage.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

import logging

from sqlalchemy.exc import DatabaseError
import luigi

from etl_tasks import (
    SqlScriptTask, UploadTask, ReportTask,
    DBAccessTask, I2B2ProjectCreate,
    TimeStampParameter)
from script_lib import Script, ChunkByBene
import script_lib as lib

log = logging.getLogger(__name__)


class CMSExtract(luigi.Task):
    download_date = TimeStampParameter(description='see luigi.cfg.example')
    cms_rif = luigi.Parameter(description='see luigi.cfg.example')
    script_variable = 'cms_source_cd'
    source_cd = "'ccwdata.org'"
    bene_chunks = luigi.IntParameter(default=1,
                                     description='see luigi.cfg.example')

    def complete(self):
        return not not self.download_date


class GrouseETL(luigi.WrapperTask):
    def reports(self):
        return [
            Demographics(),
            Encounters(),
            Diagnoses(),
        ]

    def requires(self):
        return self.reports()


class GrouseRollback(DBAccessTask):
    def complete(self):
        return False

    def requires(self):
        return []

    def run(self):
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

            for (ty, name) in objects:
                try:
                    work.execute('drop {ty} {name}'.format(ty=ty, name=name))
                    log.info('dropped %s', (ty, name))
                except DatabaseError:
                    pass


def _deep_requires(t):
    yield t
    for child in luigi.task.flatten(t.requires()):
        for anc in _deep_requires(child):
            yield anc


class FromCMS(object):
    '''Mix in source and substitution variables for CMS ETL scripts.

    Note project attribute is supplied by UploadTask.
    '''

    @property
    def source(self):
        return CMSExtract()

    @property
    def variables(self):
        return self.vars_for_deps

    @property
    def vars_for_deps(self):
        config = [(lib.I2B2STAR, self.project.star_schema),
                  (lib.CMS_RIF, self.source.cms_rif)]
        design = [(CMSExtract.script_variable, CMSExtract.source_cd)]
        return dict(config + design)


class _MappingTask(FromCMS, UploadTask):
    def requires(self):
        return UploadTask.requires(self) + [self.source]


class _DimensionTask(FromCMS, UploadTask):
    def requires(self):
        return SqlScriptTask.requires(self) + self.mappings()


class _FactLoadTask(FromCMS, UploadTask):
    script = Script.cms_facts_load

    @property
    def label(self):
        return self.txform.title

    @property
    def variables(self):
        return dict(self.vars_for_deps,
                    fact_view=self.fact_view)

    def requires(self):
        mappings = [_BeneGroupSourceMapping(), EncounterMapping()]
        txform = SqlScriptTask(
            script=self.txform,
            variables=self.vars_for_deps)
        return SqlScriptTask.requires(self) + mappings + [txform]


class _BeneChunked(FromCMS):
    group_qty = luigi.IntParameter()
    group_num = luigi.IntParameter()
    # TODO: enhance lookup_sql to support multiple sources
    bene_id_source = luigi.Parameter()

    def chunks(self, names_present):
        if not ChunkByBene.required_params <= set(names_present):
            return [{}]

        qty = self.source.bene_chunks
        first, last = ChunkByBene.group_chunks(
            qty, self.group_qty, self.group_num)
        with self.dbtrx() as q:
            result = q.execute(ChunkByBene.lookup_sql,
                               source=self.bene_id_source, qty=qty,
                               first=first, last=last).fetchall()
            bounds, sizes = ChunkByBene.result_chunks(result)
            log.info('chunks: %d thru %d sizes: %s...',
                     first, last, sizes[:3])
        return bounds


class BeneIdSurvey(FromCMS, SqlScriptTask):
    group_qty = luigi.IntParameter()
    bene_id_source = luigi.Parameter()

    script = Script.bene_chunks_survey

    @property
    def variables(self):
        return dict(
            self.vars_for_deps,
            bene_id_source=self.bene_id_source,
            chunk_qty=self.source.bene_chunks)

    @property
    def vars_for_deps(self):
        config = [(lib.CMS_RIF, self.source.cms_rif)]
        return dict(config,
                    bene_id_source=self.bene_id_source,
                    chunk_qty=self.source.bene_chunks)

    def run(self):
        SqlScriptTask.run(self, script_params=dict(
            bene_id_source=self.bene_id_source,
            chunk_qty=self.source.bene_chunks))


class _BeneGroupSourceMapping(_BeneChunked, UploadTask):
    '''Patient mapping for one group from one source table.
    '''
    script = Script.cms_patient_mapping

    def requires(self):
        survey = BeneIdSurvey(
            group_qty=self.group_qty,
            bene_id_source=self.bene_id_source)
        return UploadTask.requires(self) + [survey, self.source]

    @property
    def variables(self):
        return dict(
            self.vars_for_deps,
            bene_id_source=self.bene_id_source)


class PatientDimensionGroup(_BeneChunked, _DimensionTask):
    # TODO: _BeneChunked
    group_qty = luigi.IntParameter()
    group_num = luigi.IntParameter()
    script = Script.cms_patient_dimension

    def mappings(self):
        return [_BeneGroupSourceMapping(group_qty=self.group_qty,
                                        group_num=self.group_num,
                                        bene_id_source=src)
                for src in ChunkByBene.sources_from(self.script)]


class Demographics(ReportTask):
    group_qty = luigi.IntParameter(default=1)
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self):
        assert self.group_qty > 0, 'TODO: PosIntParamter'
        [src] = ChunkByBene.sources_from(PatientDimensionGroup.script)
        groups = [
            PatientDimensionGroup(group_qty=self.group_qty,
                                  group_num=num,
                                  bene_id_source=src)
            for num in range(1, self.group_qty + 1)]
        report = SqlScriptTask(script=self.script,
                               variables=groups[0].vars_for_deps)
        return [report] + groups


class EncounterMapping(_BeneChunked, _MappingTask):
    script = Script.cms_encounter_mapping


class VisitDimension(_DimensionTask):
    script = Script.cms_visit_dimension

    def mappings(self):
        return [_BeneGroupSourceMapping(), EncounterMapping()]


class Encounters(ReportTask):
    script = Script.cms_enc_dstats
    report_name = 'encounters_per_visit_patient'

    @property
    def data_task(self):
        return VisitDimension()


class DiagnosesLoad(_BeneChunked, _FactLoadTask):
    fact_view = 'observation_fact_cms_dx'
    txform = Script.cms_dx_txform

    @property
    def chunk_source(self):
        return '''
        (select distinct bene_id from {cms_rif}.bcarrier_claims)
        '''.format(cms_rif=self.source.cms_rif)


class Diagnoses(ReportTask):
    script = Script.cms_dx_dstats
    report_name = 'dx_by_enc_type'

    @property
    def data_task(self):
        return DiagnosesLoad()


if __name__ == '__main__':
    luigi.build([GrouseETL()], local_scheduler=True)
