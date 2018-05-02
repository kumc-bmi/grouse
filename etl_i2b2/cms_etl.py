'''cms_etl -- Load an i2b2 star schema from CMS RIF data

See README.md background and usage.

Integration Test Usage:

  (grouse-etl)$ python cms_etl.py

'''

from datetime import datetime
from typing import Iterable, List, cast
import logging

from luigi.parameter import FrozenOrderedDict
from sqlalchemy import MetaData
from sqlalchemy.engine import RowProxy
from sqlalchemy.exc import DatabaseError
import luigi

from etl_tasks import (
    SqlScriptTask, UploadTask, SourceTask,
    DBAccessTask, DBTarget, LoggedConnection, SchemaTarget,
    I2B2Task, TimeStampParameter)
from param_val import StrParam, IntParam
import param_val as pv
from script_lib import Script
import script_lib as lib
from sql_syntax import Environment, Params

log = logging.getLogger(__name__)
TimeStampParam = pv._valueOf(datetime(2001, 1, 1, 0, 0, 0), TimeStampParameter)


class CMSExtract(SourceTask, DBAccessTask):
    download_date = TimeStampParam(description='see client.cfg')
    cms_rif = StrParam(description='see client.cfg')

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

    def run(self) -> None:
        raise NotImplementedError(
            'cannot find %s.%s. CMS Extract is built elsewhere.' % (
                self.cms_rif, self.table_eg))


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


class _DimensionTask(FromCMS, UploadTask):
    @property
    def mappings(self) -> List[luigi.Task]:
        return []

    def requires(self) -> List[luigi.Task]:
        return SqlScriptTask.requires(self) + self.mappings


class BeneIdSurvey(FromCMS, SqlScriptTask):
    script = Script.bene_chunks_survey
    bene_chunks = IntParam(default=200,
                           description='see client.cfg')
    bene_chunk_max = IntParam(default=None,
                              description='see client.cfg')

    @property
    def variables(self) -> Environment:
        config = [(lib.CMS_RIF, self.source.cms_rif)]
        return dict(config,
                    chunk_qty=str(self.bene_chunks))

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(
            chunk_qty=self.bene_chunks))

    def results(self) -> List[RowProxy]:
        with self.connection(event='survey results') as lc:
            q = '''
              select chunk_num
                , bene_id_qty
                , bene_id_first
                , bene_id_last
              from bene_chunks
              where chunk_qty = :chunk_qty
                and (:chunk_max is null or
                     chunk_num <= :chunk_max)
              order by chunk_num
            '''
            params = dict(chunk_max=self.bene_chunk_max,
                          chunk_qty=self.bene_chunks)  # type: Params
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


class MappingReset(FromCMS, UploadTask):
    script = Script.mapping_reset


class MedparMapping(FromCMS, UploadTask):
    script = Script.medpar_encounter_map
    resources = {'encounter_mapping': 1}

    def requires(self) -> List[luigi.Task]:
        reset = MappingReset()
        return UploadTask.requires(self) + [self.source, reset]

    @property
    def variables(self) -> Environment:
        return self.vars_for_deps


if __name__ == '__main__':
    luigi.build([MedparMapping()], local_scheduler=True)
