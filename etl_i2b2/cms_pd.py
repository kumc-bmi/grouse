"""cms_pd -- CMS ETL using pandas (WIP)

As detailed in the example log below, the steps of an `DataLoadTask` are:

 - Allocate an upload_id and insert an upload_status record
 - Connect and select a rows from the input table bounded by bene_id;
   logging the execution plan first.
 - For each chunk of several thousand of such rows:
   - stack diagnoses and pivot facts
   - map patients and encounters
   - bulk insert into observation_fact_N where N is the upload_id

 - encounter_num: see pat_day_rollup
 - instance_num: preserves correlation with source record ("subencounter")
                 low 3 digits are dx/px number

17:37:02 00 [1] upload job: <oracle://me@db-server/sgrouse>...
17:37:02 00.002542 [1, 2] scalar select I2B2.sq_uploadstatus_uploadid.nextval...
1720 for CarrierClaimUpload #1 of 64; 121712 bene_ids
17:37:02 00.005157 [1, 2] scalar select I2B2.sq_uploadstatus_uploadid.nextval.
17:37:02 00.010149 [1, 3] execute INSERT INTO "I2B2".upload_status ...
17:37:02 00.003866 [1, 3] execute INSERT INTO "I2B2".upload_status .
17:37:02 INFO  1720 for CarrierClaimUpload #-1 of -1; 121712 bene_ids 1...
17:37:02 00.042701 [1, 4] UP#1720: ETL chunk from CMS_DEID.bcarrier_claims...
17:37:02 00.043673 [1, 4, 5] get facts...
17:37:02 00.196869 [1, 4, 5, 6] execute explain plan for SELECT * ...
17:37:02 00.007902 [1, 4, 5, 6] execute explain plan for SELECT * .
17:37:02 00.205344 [1, 4, 5, 7] execute SELECT PLAN_TABLE_OUTPUT line FROM TABLE(DBMS_XPLAN.DISPLAY())...
17:37:02 00.013324 [1, 4, 5, 7] execute SELECT PLAN_TABLE_OUTPUT line FROM TABLE(DBMS_XPLAN.DISPLAY()).
17:37:02 INFO get chunk [1031320, 1]
query: SELECT *
WHERE "CMS_DEID".bcarrier_claims.bene_id BETWEEN :bene_id_1 AND :bene_id_2 plan:
Plan hash value: 2999476641

---------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name                | Rows  | Bytes | Cost (%CPU)| Time     | Inst   |
---------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT REMOTE              |                     |  1113K|   199M|  2013K  (1)| 00:01:19 |        |
|*  1 |  FILTER                              |                     |       |       |            |          |        |
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| BCARRIER_CLAIMS     |  1113K|   199M|  2013K  (1)| 00:01:19 | SGROU~ |
|*  3 |    INDEX RANGE SCAN                  | CMS_IX_BCACLA_BENID |  2009K|       |  5406   (1)| 00:00:01 | SGROU~ |
---------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   1 - filter(:BENE_ID_2>=:BENE_ID_1)
   3 - access("BCARRIER_CLAIMS"."BENE_ID">=:BENE_ID_1 AND "BCARRIER_CLAIMS"."BENE_ID"<=:BENE_ID_2)

Note
-----
   - fully remote statement

17:37:02 00.224509 [1, 4, 5, 8] UP#1720: select from CMS_DEID.bcarrier_claims...
17:37:09 06.749958 [1, 4, 5, 8] UP#1720: select from CMS_DEID.bcarrier_claims
 + 100000 rows = 100000 for 1399 (1.15%) of 121712 bene_ids.
17:37:09 06.975701 [1, 4, 5, 9] stack diagnoses from 100000 CMS_DEID.bcarrier_claims records...
17:37:12 02.963609 [1, 4, 5, 9] stack diagnoses from 100000 CMS_DEID.bcarrier_claims records 333580 diagnoses.
17:37:12 09.940432 [1, 4, 5, 10] pivot facts from 100000 CMS_DEID.bcarrier_claims records...
17:37:22 10.808200 [1, 4, 5, 10] pivot facts from 100000 CMS_DEID.bcarrier_claims records 2767740 total observations.
17:37:23 21.472786 [1, 4, 5, 11] mapping 2767740 facts...
17:37:23 21.474279 [1, 4, 5, 11, 12] read_sql select patient_ide bene_id, patient_num from I2B2.patient_mapping
{'bene_id_last': '1001496', 'bene_id_first': '1', 'patient_ide_source': 'ccwdata.org(BENE_ID)'}...
17:37:23 00.315361 [1, 4, 5, 11, 12] read_sql select patient_ide bene_id, patient_num from I2B2.patient_mapping
{'bene_id_last': '1001496', 'bene_id_first': '1', 'patient_ide_source': 'ccwdata.org(BENE_ID)'}.
17:37:24 22.876878 [1, 4, 5, 11, 13] read_sql select medpar.medpar_id, medpar.bene_id, emap.encounter_num
{'bene_id_last': '1001496', 'bene_id_first': '1', 'encounter_ide_source': 'ccwdata.org(MEDPAR_ID)'}...
17:37:25 00.389943 [1, 4, 5, 11, 13] read_sql select medpar.medpar_id, medpar.bene_id, emap.encounter_num
{'bene_id_last': '1001496', 'bene_id_first': '1', 'encounter_ide_source': 'ccwdata.org(MEDPAR_ID)'}.
17:37:41 17.737663 [1, 4, 5, 11] mapping 2767740 facts pmap: 16627 emap: 2121.
17:37:43 40.980482 [1, 4, 5] get facts 2767740 facts.
17:37:43 41.025014 [1, 4, 14] UP#1720: bulk insert 2767740 rows into observation_fact_1720...
17:38:17 34.165037 [1, 4, 14] UP#1720: bulk insert 2767740 rows into observation_fact_1720.
17:38:17 01:15.148141 [1, 4] UP#1720: ETL chunk from CMS_DEID.bcarrier_claims.
17:38:17 01:15.191470 [1] upload job: <oracle://me@db-server/sgrouse>

"""

from io import StringIO
from typing import Iterator, List, Dict, Optional as Opt, Tuple, Type, TypeVar, cast
import enum

import cx_ora_fix; cx_ora_fix.patch_version()  # noqa: E702

import luigi
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import pkg_resources as pkg
import sqlalchemy as sqla

from cms_etl import FromCMS, DBAccessTask, BeneIdSurvey, PatientMapping, MedparMapping, ReportTask
from etl_tasks import LoggedConnection, LogState, SqlScriptTask, UploadTarget, UploadTask, make_url, log_plan
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Params

T = TypeVar('T')


class CMSRIFLoad(luigi.WrapperTask):
    def requires(self) -> List[luigi.Task]:
        return [
            DemographicSummaries(),
            InpatientStays(),
            MedRx(),
            CarrierClaims(),
            OutpatientClaims(),
        ]


class DataLoadTask(FromCMS, DBAccessTask):
    @property
    def label(self) -> str:
        raise NotImplementedError

    @property
    def input_label(self) -> str:
        raise NotImplementedError

    def output(self) -> luigi.Target:
        return self._upload_target()

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self._make_url(self.account),
                            self.project.upload_table,
                            self.task_id, self.source,
                            echo=self.echo)

    def run(self) -> None:
        upload = self._upload_target()
        with upload.job(self,
                        label=self.label,
                        user_id=make_url(self.account).username) as conn_id_r:
            lc, upload_id, result = conn_id_r
            fact_table = sqla.Table('observation_fact_%s' % upload_id,
                                    sqla.MetaData(),
                                    *[c.copy() for c in self.project.observation_fact_columns],
                                    oracle_compress=True)
            fact_table.create(lc._conn)
            fact_dtype = {c.name: c.type for c in fact_table.columns
                          if not c.name.endswith('_blob')}
            bulk_rows = 0
            obs_fact_chunks = self.obs_data(lc, upload_id)
            while 1:
                with lc.log.step('UP#%(upload_id)d: %(event)s from %(input)s',
                                 dict(event='ETL chunk', upload_id=upload_id,
                                      input=self.input_label)):
                    with lc.log.step('%(event)s',
                                     dict(event='get facts')) as step1:
                        try:
                            obs_fact_chunk, pct_in = next(obs_fact_chunks)
                        except StopIteration:
                            break
                        step1.msg_parts.append(' %(fact_qty)s facts')
                        step1.argobj.update(dict(fact_qty=len(obs_fact_chunk)))
                    with lc.log.step('UP#%(upload_id)d: %(event)s %(rowcount)d rows into %(into)s',
                                     dict(event='bulk insert',
                                          upload_id=upload_id,
                                          into=fact_table.name,
                                          rowcount=len(obs_fact_chunk))) as insert_step:
                        obs_fact_chunk.to_sql(name=fact_table.name,
                                              con=lc._conn,
                                              dtype=fact_dtype,
                                              if_exists='append', index=False)
                        bulk_rows += len(obs_fact_chunk)
                        insert_step.argobj.update(dict(rowsubtotal=bulk_rows))
                        insert_step.msg_parts.append(
                            ' (subtotal: %(rowsubtotal)d)')

                    # report progress via the luigi scheduler and upload_status table
                    _start, elapsed, elapsed_ms = lc.log.elapsed()
                    eta = lc.log.eta(pct_in)
                    message = ('UP#%(upload_id)d %(pct_in)0.2f%% eta %(eta)s '
                               'loaded %(bulk_rows)d rows @%(rate_out)0.2fK/min %(elapsed)s') % dict(
                        upload_id=upload_id, pct_in=pct_in, eta=eta.strftime('%a %d %b %H:%M'),
                        bulk_rows=bulk_rows, elapsed=elapsed,
                        rate_out=bulk_rows / 1000.0 / (elapsed_ms / 1000000.0 / 60))
                    self.set_status_message(message)
                    lc.execute(upload.table.update()
                               .where(upload.table.c.upload_id == upload_id)
                               .values(loaded_record=bulk_rows, end_date=eta,
                                       message=message))
            result[upload.table.c.loaded_record.name] = bulk_rows

    def obs_data(self, lc: LoggedConnection, upload_id: int) -> Iterator[Tuple[pd.DataFrame, float]]:
        raise NotImplementedError


def read_sql_step(sql: str, lc: LoggedConnection, params: Params) -> pd.DataFrame:
    with lc.log.step('%(event)s %(sql1)s' + ('\n%(params)s' if params else ''),
                     dict(event='read_sql', sql1=str(sql).split('\n')[0], params=params)):
        return pd.read_sql(sql, lc._conn, params=params or {})


class BeneMapped(DataLoadTask):
    def requires(self) -> List[luigi.Task]:
        return [PatientMapping()]

    def complete(self) -> bool:
        return (self.output().exists() and
                all(task.complete() for task in self.requires()))

    def ide_source(self, key_cols: str) -> str:
        source_cd = self.source.source_cd[1:-1]  # strip quotes
        return source_cd + key_cols

    def patient_mapping(self, lc: LoggedConnection,
                        bene_range: Tuple[int, int],
                        debug_plan: bool=False,
                        key_cols: str='(BENE_ID)') -> pd.DataFrame:
        q = '''select patient_ide bene_id, patient_num from %(I2B2STAR)s.patient_mapping
        where patient_ide_source = :patient_ide_source
        and patient_ide between :bene_id_first and :bene_id_last
        ''' % dict(I2B2STAR=self.project.star_schema)

        params = dict(patient_ide_source=self.ide_source(key_cols),
                      bene_id_first=bene_range[0],
                      bene_id_last=bene_range[1])  # type: Params
        if debug_plan:
            log_plan(lc, event='patient_mapping', sql=q, params=params)
        return read_sql_step(q, lc, params=params)


class MedparMapped(BeneMapped):
    def requires(self) -> List[luigi.Task]:
        return BeneMapped.requires(self) + [MedparMapping()]

    def encounter_mapping(self, lc: LoggedConnection,
                          bene_range: Tuple[int, int],
                          debug_plan: bool=False,
                          key_cols: str='(MEDPAR_ID)') -> pd.DataFrame:
        q = '''select medpar.medpar_id, medpar.bene_id, emap.encounter_num
                    , medpar.admsn_dt, medpar.dschrg_dt
        from %(CMS_RIF)s.medpar_all medpar
        join %(I2B2STAR)s.encounter_mapping emap on emap.encounter_ide = medpar.medpar_id
        where medpar.bene_id between :bene_id_first and :bene_id_last
          and emap.patient_ide between :bene_id_first and :bene_id_last
          and emap.encounter_ide_source = :encounter_ide_source
        ''' % dict(I2B2STAR=self.project.star_schema,
                   CMS_RIF=self.source.cms_rif)

        params = dict(encounter_ide_source=self.ide_source(key_cols),
                      bene_id_first=bene_range[0],
                      bene_id_last=bene_range[1])  # type: Params

        if debug_plan:
            log_plan(lc, event='patient_mapping', sql=q, params=params)

        return read_sql_step(q, lc, params=params)

    @classmethod
    def pat_day_rollup(cls, data: pd.DataFrame, medpar_mapping: pd.DataFrame) -> pd.DataFrame:
        """
        :param data: with bene_id, start_date, and optionally medpar_id
        :param medpar_mapping: with medpar_id, encounter_num, admsn_dt, dschrg_dt

        Note medpar_mapping.sql ensures encounter_num > 0 when assigned to a medpar_id.
        """
        out = data.reset_index().copy()
        out['start_day'] = pd.to_datetime(out.start_date, unit='D')
        pat_day = out[['bene_id', 'start_day']].drop_duplicates()

        # assert(medpar_mapping is 1-1 from medpar_id to encounter_num)
        pat_enc = pat_day.merge(medpar_mapping, on='bene_id', how='left')

        pat_enc = pat_enc[(pat_enc.start_day >= pat_enc.admsn_dt) &
                          (pat_enc.start_day <= pat_enc.dschrg_dt)]
        pat_enc = pat_enc.set_index(['bene_id', 'start_day'])  # [['encounter_num', 'medpar_id']]
        pat_enc = pat_enc[~pat_enc.index.duplicated(keep='first')]
        out = out.merge(pat_enc, how='left', left_on=['bene_id', 'start_day'], right_index=True)
        assert len(out) == len(data)

        # ISSUE: hash is not portable between python and Oracle
        fallback = - cls.fmt_patient_day(out).apply(hash).abs()
        out.encounter_num = out.encounter_num.fillna(fallback)

        return out

    @classmethod
    def fmt_patient_day(cls, df: pd.DataFrame) -> pd.Series:
        return df.start_date.dt.strftime('%Y-%m-%d') + ' ' + df.bene_id

    def with_mapping(self, data: pd.DataFrame,
                     pmap: pd.DataFrame, emap: pd.DataFrame) -> pd.DataFrame:
        obs = data.merge(pmap, on=CMSVariables.bene_id)

        if 'medpar_id' in data.columns.values:
            obs = obs.merge(emap, on=CMSVariables.medpar_id, how='left')
        else:
            obs = self.pat_day_rollup(obs, emap)

        if 'provider_id' in obs.columns.values:
            obs.provider_id = obs.provider_id.where(~obs.provider_id.isnull(), '@')
        else:
            obs['provider_id'] = '@'

        return obs


class CMSVariables(object):
    r'''CMS Variables are more or less the same as SQL columns.

    We curate active columns (variables):

    >>> CMSVariables.active_columns('PDE_SAF')[
    ...     ['Status', 'table_name', 'column_name', 'description']].head(2)
        Status table_name column_name            description
    415      A    pde_saf      PDE_ID   Encrypted 723 PDE ID
    417      A    pde_saf     SRVC_DT  RX Service Date (DOS)

    We relate columns to i2b2 `valtype_cd` typically by SQL type but
    subclasses may use `valtype_override` to map column_name (matched
    by regexp) to valtype_cd.

    Columns in the range of `cls.i2b2_map` get valtype_cd = np.nan, indicating
    that they don't serve as facts.
    '''
    i2b2_map = {
        'patient_ide': 'bene_id',
        'start_date': 'clm_from_dt',
        'end_date': 'clm_thru_dt',
        'update_date': 'nch_wkly_proc_dt'}

    bene_id = 'bene_id',
    medpar_id = 'medpar_id'

    pdx = 'prncpal_dgns_cd'

    """Tables all have less than 10^3 columns."""
    max_cols_digits = 3

    """Columns shorter than this are treated as codes. """
    code_max_len = 7

    valtype_override = []  # type: List[Tuple[str, str]]
    concept_scheme_override = {'hcpcs_cd': 'HCPCS'}
    _mute_unused_warning = Dict

    _active_columns = pkg.resource_string(__name__, 'metadata/active_columns.csv')

    @classmethod
    def active_columns(cls, table_name: str,
                       active: str='A') -> pd.DataFrame:
        col_info = pd.read_csv(StringIO(cls._active_columns.decode('utf-8')))
        return col_info[(col_info.table_name == table_name.lower()) &
                        (col_info.Status == active)]

    @classmethod
    def column_properties(cls, info: pd.DataFrame) -> pd.DataFrame:
        '''Relate columns (variables) to i2b2 `valtype_cd`.
        '''
        info['valtype_cd'] = [col_valtype(c).value for c in info.column.values]

        for cd, pat in cls.valtype_override:
            info.valtype_cd = info.valtype_cd.where(~ info.column_name.str.match(pat), cd)
        info.loc[info.column_name.isin(cls.i2b2_map.values()), 'valtype_cd'] = np.nan

        return info.drop('column', 1)


def rif_modifier(table_name: str) -> str:
    return 'CMS_RIF:' + table_name.upper()


@enum.unique
class Valtype(enum.Enum):
    """cf section 3.2 Observation_Fact of i2b2 CRC Design
    """
    coded = '@'
    text = 'T'
    date = 'D'
    numeric = 'N'


@enum.unique
class NumericOp(enum.Enum):
    """cf section 3.2 Observation_Fact of i2b2 CRC Design
    """
    eq = 'E'
    not_eq = 'NE'
    lt = 'L'
    lt_or_eq = 'LE'
    gt = 'G'
    gt_or_eq = 'GE'


@enum.unique
class PDX(enum.Enum):
    """cf. PCORNet CDM"""
    primary = '1'
    secondary = '2'


def col_valtype(col: sqla.Column) -> Valtype:
    """Determine valtype_cd based on measurement level
    """
    return (
        Valtype.numeric
        if isinstance(col.type, sqla.types.Numeric) else
        Valtype.date
        if isinstance(col.type, (sqla.types.Date, sqla.types.DateTime)) else
        Valtype.text if (isinstance(col.type, sqla.types.String) and
                         col.type.length > CMSVariables.code_max_len) else
        Valtype.coded
    )


def col_groups(col_info: pd.DataFrame,
               suffixes: List[str]) -> pd.DataFrame:
    out = None
    for ix, suffix in enumerate(suffixes):
        cols = col_info[ix::len(suffixes)].reset_index()[['column_name']]
        if out is None:
            out = cols
        else:
            out = out.merge(cols, left_index=True, right_index=True)
    if out is None:
        raise TypeError('no suffixes?')
    out.columns = ['column_name' + s for s in suffixes]
    return out


def fmt_dx_codes(dgns_vrsn: pd.Series, dgns_cd: pd.Series,
                 decimal_pos: int=3) -> pd.Series:
    #   I found null dgns_vrsn e.g. one record with ADMTG_DGNS_CD = V5789
    #   so let's default to the IDC9 case
    scheme = 'ICD' + dgns_vrsn.where(~dgns_vrsn.isnull(), '9')
    decimal = np.where(dgns_cd.str.len() > decimal_pos, '.', '')
    before = dgns_cd.str.slice(stop=decimal_pos)
    after = dgns_cd.str.slice(start=decimal_pos)
    return scheme + ':' + before + decimal + after


def fmt_px_codes(prcdr_cd: pd.Series, prcdr_vrsn: pd.Series) -> pd.Series:
    # TODO: ICDC10??
    out = np.where(prcdr_vrsn.isin(['CPT', 'HCPCS']),
                   'CPT:' + prcdr_cd,
                   'ICD9:' + np.where(prcdr_cd.str.len() > 2,
                                      prcdr_cd.str[:2] + '.' + prcdr_cd.str[2:],
                                      prcdr_cd))
    return out


class CMSRIFUpload(MedparMapped, CMSVariables):
    bene_id_first = IntParam()
    bene_id_last = IntParam()
    chunk_rows = IntParam(significant=False, default=-1)
    group_num = IntParam(significant=False, default=-1)
    group_qty = IntParam(significant=False, default=-1)

    chunk_size = IntParam(default=10000, significant=False)
    # label doesn't overlap with RIF columns
    src_ix = sqla.literal_column('rownum', type_=sqla.types.Integer).label('src_ix')

    table_name = 'PLACEHOLDER'

    obs_id_vars = ['patient_ide', 'start_date', 'end_date', 'update_date', 'provider_id']
    obs_value_cols = ['update_date', 'start_date', 'end_date']

    @property
    def label(self) -> str:
        return ('%(task_family)s #%(group_num)s of %(group_qty)s;'
                ' %(chunk_rows)s rows' %
                dict(self.to_str_params(), task_family=self.task_family))

    @property
    def input_label(self) -> str:
        return self.qualified_name()

    def qualified_name(self, name: Opt[str] = None) -> str:
        return '%s.%s' % (self.source.cms_rif, name or self.table_name)

    def table_info(self, lc: LoggedConnection) -> sqla.MetaData:
        return self.source.table_details(lc, [self.table_name])

    def source_query(self, meta: sqla.MetaData) -> sqla.sql.expression.Select:
        # ISSUE: order_by(t.c.bene_id)?
        t = meta.tables[self.qualified_name()].alias('rif')
        return (sqla.select([self.src_ix] + self.active_source_cols(t))  # type: ignore
                .where(t.c.bene_id.between(self.bene_id_first, self.bene_id_last)))

    def active_source_cols(self, t: sqla.Table) -> List[sqla.Column]:
        active_col_names = CMSVariables.active_columns(self.table_name).column_name.str.lower()
        return [c for c in t.columns
                if c.name in active_col_names.values or
                c.name in self.i2b2_map.values()]

    def chunks(self, lc: LoggedConnection,
               chunk_size: int=1000) -> pd.DataFrame:
        params = dict(bene_id_first=self.bene_id_first,
                      bene_id_last=self.bene_id_last)
        meta = self.table_info(lc)
        q = self.source_query(meta)
        log_plan(lc, event='get chunk', query=q, params=params)
        return pd.read_sql(q, lc._conn, params=params, chunksize=chunk_size)

    def column_data(self, lc: LoggedConnection) -> pd.DataFrame:
        meta = self.table_info(lc)
        q = self.source_query(meta)

        return pd.DataFrame([dict(column_name=col.name,
                                  data_type=col.type,
                                  column=col)
                             for col in q.columns
                             if col.name != self.src_ix.name])

    def obs_data(self, lc: LoggedConnection, upload_id: int) -> Iterator[Tuple[pd.DataFrame, float]]:
        cols = self.column_properties(self.column_data(lc))
        chunks = self.chunks(lc, chunk_size=self.chunk_size)
        subtot_in = 0

        bene_range = (self.bene_id_first, self.bene_id_last)
        with lc.log.step('%(event)s %(bene_range)s',
                         dict(event='mapping', bene_range=bene_range)) as map_step:
            pmap = self.patient_mapping(lc, bene_range)
            map_step.argobj.update(pmap_len=len(pmap))
            map_step.msg_parts.append(' pmap: %(pmap_len)d')
            emap = self.encounter_mapping(lc, bene_range)
            map_step.argobj.update(emap_len=len(emap))
            map_step.msg_parts.append(' emap: %(emap_len)d')

        while 1:
            with lc.log.step('UP#%(upload_id)d: %(event)s from %(source_table)s',
                             dict(event='select', upload_id=upload_id,
                                  source_table=self.qualified_name())) as s1:
                try:
                    data = next(chunks).set_index(self.src_ix.name)
                except StopIteration:
                    break
                subtot_in, pct_in = self._input_progress(data, subtot_in, s1)

            obs = self.custom_obs(lc, data, cols)

            with lc.log.step('%(event)s from %(records)d %(source_table)s records',
                             dict(event='pivot facts', records=len(data),
                                  source_table=self.qualified_name())) as pivot_step:
                for valtype in Valtype:
                    obs_v = self.pivot_valtype(valtype, data, self.table_name, cols)
                    if len(obs_v) > 0:
                        obs = obs_v if obs is None else obs.append(obs_v)
                if obs is None:
                    continue
                pivot_step.argobj.update(dict(obs_len=len(obs)))
                pivot_step.msg_parts.append(' %(obs_len)d total observations')

                mapped = self.with_mapping(obs, pmap, emap)

            current_time = pd.read_sql(sqla.select([sqla.func.current_timestamp()]),
                                       lc._conn).iloc[0][0]
            obs_fact = self.with_admin(mapped, upload_id=upload_id, import_date=current_time)

            yield obs_fact, pct_in

    def _input_progress(self, data: pd.DataFrame,
                        subtot_in: int,
                        s1: LogState) -> Tuple[int, float]:
        subtot_in += len(data)
        pct_in = 100.0 * subtot_in / self.chunk_rows
        s1.argobj.update(rows_in=len(data), subtot_in=subtot_in, pct_in=pct_in,
                         chunk_rows=self.chunk_rows)
        s1.msg_parts.append(
            ' + %(rows_in)d rows = %(subtot_in)d (%(pct_in)0.2f%%) of %(chunk_rows)d')
        return subtot_in, pct_in

    @classmethod
    def _map_cols(cls, obs: pd.DataFrame, i2b2_cols: List[str],
                  required: bool=False) -> pd.DataFrame:
        """
        Note: cls.i2b2_map may map more than one rif col to an i2b2 col.
        So we don't bother to get rid of the old column.
        """
        out = obs.copy()
        for c in i2b2_cols:
            if required or c in cls.i2b2_map:
                out[c] = obs[cls.i2b2_map[c]]

        return out

    def custom_obs(self, lc: LoggedConnection,
                   data: pd.DataFrame, cols: pd.DataFrame) -> Opt[pd.DataFrame]:
        return None

    @classmethod
    def pivot_valtype(cls, valtype: Valtype, rif_data: pd.DataFrame,
                      table_name: str, col_info: pd.DataFrame) -> pd.DataFrame:
        '''Unpivot columns of (wide) rif_data with a given i2b2 valtype to (long) i2b2 facts.

        The i2b2 `concept_cd` scheme is taken from the RIF column name.

        Facts from the same row of rif_data are given a common `modifier_cd`.

        The `provider_id`, `start_date` etc. are mapped using CMSVariables.i2b2_map.

        @param table_name: used with `rif_modifier` to make fact `modifier_cd`
        @param col_info: with column_name, valtype_cd columns

        See also `DataFrame.melt`__.

        __ https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.melt.html
        '''
        id_vars = _no_dups([cls.i2b2_map[v] for v in cls.obs_id_vars if v in cls.i2b2_map])
        ty_cols = list(col_info[col_info.valtype_cd == valtype.value].column_name)
        ty_data = rif_data[id_vars + ty_cols].copy()

        # please excuse one line of code duplication...
        spare_digits = CMSVariables.max_cols_digits
        ty_data['instance_num'] = ty_data.index * (10 ** spare_digits)

        # i2b2 numeric (and text?) constraint searches only match modifier_cd = '@'
        # so only use rif_modifer() on coded values.
        ty_data['modifier_cd'] = rif_modifier(table_name) if valtype == Valtype.coded else '@'

        obs = ty_data.melt(id_vars=id_vars + ['instance_num', 'modifier_cd'],
                           var_name='column').dropna(subset=['value'])

        V = Valtype
        obs['valtype_cd'] = valtype.value
        scheme = obs.column.apply(
            lambda name: cls.concept_scheme_override.get(name, name)).str.upper() + ':'
        if valtype == V.coded:
            obs['concept_cd'] = scheme + obs.value
            obs['tval_char'] = None  # avoid NaN, which causes sqlalchemy to choke
        else:
            obs['concept_cd'] = scheme
            if valtype == V.numeric:
                obs['nval_num'] = obs.value
                obs['tval_char'] = NumericOp.eq.value
            elif valtype == V.text:
                obs['tval_char'] = obs.value
            elif valtype == V.date:
                obs['tval_char'] = obs.value.astype('<U')  # format yyyy-mm-dd...
            else:
                raise TypeError(valtype)

        if valtype == V.date:
            obs['start_date'] = obs['end_date'] = obs.value
        else:
            obs = cls._map_cols(obs, ['start_date', 'end_date'])

        obs = cls._map_cols(obs, ['update_date'], required=True)
        obs = cls._map_cols(obs, ['provider_id'])

        return obs

    def with_admin(self, mapped: pd.DataFrame,
                   import_date: object, upload_id: int) -> pd.DataFrame:
        obs_fact = mapped[[col.name for col in self.project.observation_fact_columns
                           if col.name in mapped.columns.values]].copy()
        obs_fact['sourcesystem_cd'] = self.source.source_cd[1:-1]  # kludgy
        obs_fact['download_date'] = self.source.download_date
        obs_fact['upload_id'] = upload_id
        obs_fact['import_date'] = import_date
        return obs_fact


def _no_dups(seq: List[T]) -> List[T]:
    from typing import Set, Callable, Any
    # ack: https://stackoverflow.com/a/480227/7963
    seen = set()  # type: Set[T]
    seen_add = seen.add  # type: Callable[[T], Any]
    return [x for x in seq if not (x in seen or seen_add(x))]
    Set, Callable, Any  # mute unused import warning


def obs_stack(rif_data: pd.DataFrame,
              rif_table_name: str, projections: pd.DataFrame,
              id_vars: List[str], value_vars: List[str]) -> pd.DataFrame:
    '''
    :param projections: columns to project (e.g. diagnosis code and version);
                        order matches value_vars
    :param id_vars: a la pandas.melt (no dups allowed)
    :param value_vars: a la melt; data column (e.g. dgns_cd) followed by dgns_vrsn etc.
    '''
    assert id_vars == _no_dups(id_vars)
    assert len(projections) >= 1

    spare_digits = CMSVariables.max_cols_digits

    out = None
    for ix, rif_cols in projections.iterrows():
        obs = rif_data[id_vars + list(rif_cols.values)].copy()

        instance_num = obs.index * (10 ** spare_digits) + ix
        obs = obs.set_index(id_vars)
        obs.columns = value_vars  # e.g. icd_dgns_cd11 -> dgns_cd
        obs['instance_num'] = instance_num

        obs = obs.dropna(subset=[value_vars[0]])

        obs['modifier_cd'] = (
            PDX.primary.value if rif_cols.values[0] == CMSVariables.pdx else
            rif_modifier(rif_table_name))

        if out is None:
            out = obs
        else:
            out = out.append(obs)

    if out is None:
        raise TypeError('no projections?')

    return out


class date_trunc(sqla.sql.functions.GenericFunction):  # type: ignore
    type = sqla.types.DateTime
    name = 'trunc'


class _ByExtractYear(CMSRIFUpload):
    bene_enrollmt_ref_yr = IntParam(default=2013)

    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='start_date',  # start of year
        end_date='extract_dt',    # end of year
        update_date='download_date')

    def source_query(self, meta: sqla.MetaData) -> sqla.sql.expression.Select:
        t = meta.tables[self.qualified_name()].alias('rif')
        download_col = sqla.literal(self.source.download_date).label('download_date')
        start_date = date_trunc(t.c.extract_dt, 'year').label('start_date')
        return (sqla.select([self.src_ix, start_date, download_col] +  # type: ignore
                            self.active_source_cols(t))
                .where(t.c.bene_id.between(self.bene_id_first, self.bene_id_last)))


class MBSFUpload(_ByExtractYear):
    table_name = 'mbsf_ab_summary'


class MAXPSUpload(_ByExtractYear):
    table_name = 'maxdata_ps'

    valtype_override = [
        ('@', '.*_cd$')  # e.g. EL_AGE_GRP_CD
    ]

    coltype_override = [
        (sqla.String(CMSVariables.code_max_len - 1), '_cd')
    ]

    def active_source_cols(self, t: sqla.Table) -> List[sqla.Column]:
        '''
        '''
        info = CMSRIFUpload.active_source_cols(self, t)
        for desired_type, suffix in self.coltype_override:
            info = [sqla.sql.expression.cast(col, desired_type).label(col.name)  # type: ignore
                    if col.name.endswith(suffix) else col
                    for col in info]
        return info


class _DxPxCombine(CMSRIFUpload):
    valtype_dx = '@dx'
    valtype_px = '@px'

    valtype_override = [
        (valtype_dx, r'.*(_dgns_|rsn_visit)'),
        (valtype_px, r'.*prcdr_')
    ]

    def custom_obs(self, lc: LoggedConnection,
                   data: pd.DataFrame, cols: pd.DataFrame) -> pd.DataFrame:
        with lc.log.step('%(event)s from %(records)d %(source_table)s records',
                         dict(event='stack dx, px', records=len(data),
                              source_table=self.qualified_name())) as stack_step:
            obs = None
            obs_dx = self.dx_data(data, self.table_name, cols)
            if obs_dx is not None:
                stack_step.msg_parts.append(' %(dx_len)d diagnoses')
                stack_step.argobj.update(dict(dx_len=len(obs_dx)))
                obs = obs_dx
            obs_px = self.px_data(data, self.table_name, cols)
            if obs_px is not None:
                stack_step.msg_parts.append(' %(px_len)d procedures')
                stack_step.argobj.update(dict(px_len=len(obs_px)))
                if obs is None:
                    obs = obs_px
                else:
                    obs = obs.append(obs_px)
        return obs

    @classmethod
    def dx_data(cls, rif_data: pd.DataFrame,
                table_name: str, col_info: pd.DataFrame) -> pd.DataFrame:
        """Combine diagnosis columns i2b2 style
        """
        dx_cols = col_groups(col_info[col_info.valtype_cd == cls.valtype_dx], ['_cd', '_vrsn'])
        if len(dx_cols) < 1:
            return None
        obs = obs_stack(rif_data, table_name, dx_cols,
                        id_vars=_no_dups([cls.i2b2_map[v]
                                          for v in cls.obs_id_vars if v in cls.i2b2_map]),
                        value_vars=['dgns_cd', 'dgns_vrsn']).reset_index()
        obs['valtype_cd'] = Valtype.coded.value
        obs['concept_cd'] = fmt_dx_codes(obs.dgns_vrsn, obs.dgns_cd)
        obs = cls._map_cols(obs, cls.obs_value_cols, required=True)
        return obs

    @classmethod
    def px_data(cls, data: pd.DataFrame, table_name: str, col_info: pd.DataFrame) -> pd.DataFrame:
        """Combine procedure columns i2b2 style
        """
        px_cols = col_groups(col_info[col_info.valtype_cd == cls.valtype_px], ['_cd', '_vrsn', '_dt'])
        if len(px_cols) < 1:
            return None
        obs = obs_stack(data, table_name, px_cols,
                        id_vars=_no_dups([cls.i2b2_map[v]
                                          for v in cls.obs_id_vars if v in cls.i2b2_map]),
                        value_vars=['prcdr_cd', 'prcdr_vrsn', 'prcdr_dt']).reset_index()
        obs['valtype_cd'] = Valtype.coded.value
        obs['concept_cd'] = fmt_px_codes(obs.prcdr_cd, obs.prcdr_vrsn)
        return obs.rename(columns=dict(prcdr_dt='start_date'))


class MAXDATA_IP_Upload(_DxPxCombine):
    table_name = 'maxdata_ip'

    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='srvc_bgn_dt',
        end_date='srvc_end_dt',
        update_date='srvc_end_dt')


class MEDPAR_Upload(_DxPxCombine):
    table_name = 'medpar_all'

    i2b2_map = dict(
        patient_ide='bene_id',
        encounter_ide='medpar_id',
        start_date='admsn_dt',
        end_date='dschrg_dt',
        update_date='ltst_clm_acrtn_dt')


class CarrierClaimUpload(_DxPxCombine):
    table_name = 'bcarrier_claims'

    # see missing Carrier Claim Billing NPI Number #8
    # https://github.com/kumc-bmi/grouse/issues/8
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='clm_from_dt',
        end_date='clm_thru_dt',
        update_date='nch_wkly_proc_dt')


class CarrierLineUpload(_DxPxCombine):
    table_name = 'bcarrier_line'
    claim_table_name = CarrierClaimUpload.table_name

    i2b2_map = dict(
        patient_ide='bene_id',
        # performance of joining with bcarrier_claims is disastrous
        # start_date='clm_from_dt',
        start_date='clm_thru_dt',
        end_date='clm_thru_dt',
        provider_id='prf_physn_npi',
        update_date='line_last_expns_dt')

    valtype_override = _DxPxCombine.valtype_override + [
        ('@', 'line_ndc_cd')
    ]
    concept_scheme_override = dict(_DxPxCombine.concept_scheme_override,
                                   line_ndc_cd='NDC')

    def _table_info_too_slow(self, lc: LoggedConnection) -> sqla.MetaData:
        return self.source.table_details(lc, [self.table_name, self.claim_table_name])

    def _source_query_too_slow(self, meta: sqla.MetaData) -> sqla.sql.expression.Select:
        line = meta.tables[self.qualified_name()].alias('line')
        claim = meta.tables[self.qualified_name(self.claim_table_name)].alias('claim')
        return (sqla.select([claim.c.clm_from_dt, line])  # type: ignore
                .select_from(line.join(claim, line.c.clm_id == claim.c.clm_id))
                .where(sqla.and_(
                    line.c.bene_id.between(self.bene_id_first, self.bene_id_last),
                    claim.c.bene_id.between(self.bene_id_first, self.bene_id_last))))


class OutpatientClaimUpload(_DxPxCombine):
    table_name = 'outpatient_base_claims'
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='clm_from_dt',
        end_date='clm_thru_dt',
        update_date='nch_wkly_proc_dt',
        provider_id='at_physn_npi')


class DrugEventUpload(CMSRIFUpload):
    table_name = 'pde_saf'
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='srvc_dt',
        end_date='srvc_dt',
        update_date='srvc_dt',
        provider_id='prscrbr_id')

    valtype_override = [
        ('@', 'prod_srvc_id')
    ]
    concept_scheme_override = {
        'prod_srvc_id': 'NDC'
    }


class MAXRxUpload(CMSRIFUpload):
    table_name = 'maxdata_rx'
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='prscrptn_fill_dt',
        end_date='prsc_wrte_dt',
        update_date='extract_dt',
        provider_id='npi')

    valtype_override = [
        ('@', 'ndc')
    ]


class _BeneIdGrouped(luigi.WrapperTask):
    group_tasks = cast(List[Type[CMSRIFUpload]], [])  # abstract

    def requires(self) -> List[luigi.Task]:
        deps = []  # type: List[luigi.Task]
        for group_task in self.group_tasks:
            survey = BeneIdSurvey()
            deps += [survey]
            results = survey.results()
            if results:
                deps += [
                    group_task(
                        group_num=ntile.chunk_num,
                        group_qty=len(results),
                        bene_id_qty=ntile.bene_id_qty,
                        bene_id_first=ntile.bene_id_first,
                        bene_id_last=ntile.bene_id_last)
                    for ntile in results
                ]
        return deps


class CarrierClaims(_BeneIdGrouped):
    group_tasks = [CarrierClaimUpload, CarrierLineUpload]


class MedRx(_BeneIdGrouped):
    group_tasks = [DrugEventUpload, MAXRxUpload]


class OutpatientClaims(_BeneIdGrouped):
    group_tasks = [OutpatientClaimUpload]


class DemographicSummaries(_BeneIdGrouped):
    group_tasks = [MBSFUpload, MAXPSUpload]


class InpatientStays(_BeneIdGrouped):
    group_tasks = [MEDPAR_Upload, MAXDATA_IP_Upload]


def obj_string(df: pd.DataFrame,
               clobs: List[str]=[],
               pad: int=4) -> Dict[str, sqla.types.String]:
    '''avoid CLOBs'''
    df = df.reset_index()
    obj_cols = [col for (col, ty) in df.dtypes.items()
                if ty.kind == 'O' and col not in clobs]
    dt = {col: sqla.types.String(np.nanmax([df[col].str.len().max() + pad, pad]))
          for col in obj_cols}
    # log.debug('no clobs? %s', dt)
    return dt


class LoadDataFile(DBAccessTask):
    table_name = StrParam()
    directory = StrParam(default='metadata')

    def complete(self) -> bool:
        table = sqla.Table(self.table_name, sqla.MetaData())
        return table.exists(bind=self._dbtarget().engine)  # type: ignore

    def run(self) -> None:
        data = pd.read_csv('%s/%s.csv' % (self.directory, self.table_name))  # ISSUE: ambient
        with self.connection('load data file') as lc:
            data.to_sql(self.table_name, lc._conn, if_exists='replace',
                        dtype=obj_string(data))


class PatientDimension(FromCMS, UploadTask):
    script = Script.cms_patient_dimension

    def requires(self) -> List[luigi.Task]:
        curated = LoadDataFile(table_name='cms_pcornet_map')
        return SqlScriptTask.requires(self) + [curated]


class Demographics(ReportTask):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self) -> List[luigi.Task]:
        pd = PatientDimension()
        report = SqlScriptTask(script=self.script,
                               param_vars=pd.vars_for_deps)  # type: luigi.Task
        return [report] + cast(List[luigi.Task], [pd])
