"""cms_pd -- CMS ETL using pandas (WIP)
"""

from typing import Iterator, List

import luigi
import numpy as np
import pandas as pd
import sqlalchemy as sqla

from cms_etl import FromCMS, DBAccessTask
from etl_tasks import LoggedConnection, UploadTarget, make_url, log_plan
from param_val import IntParam


class DataLoadTask(DBAccessTask):
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
            bulk_rows = 0
            for obs_fact_chunk in self.obs_data(lc, upload_id):
                obs_fact_chunk.to_sql(name='observation_fact_%s' % upload_id,
                                      con=lc._conn,
                                      if_exists='append', index=False)
                bulk_rows += len(obs_fact_chunk)

            result[upload.table.c.loaded_record.name] = bulk_rows

    def obs_data(self, upload_id: int) -> Iterator[pd.DataFrame]:
        raise NotImplementedError


class BeneMapped(FromCMS, DataLoadTask):

    @property
    def patient_ide_source(self,
                           key_cols='(BENE_ID)') -> str:
        source_cd = self.source.source_cd[1:-1]  # strip quotes
        return source_cd + key_cols

    def patient_mapping(self, lc: LoggedConnection,
                        bene_id_first: int, bene_id_last: int,
                        key_cols='(BENE_ID)') -> pd.DataFrame:
        # TODO: use sqlalchemy API
        q = '''
        select patient_ide bene_id, patient_num
        from %(I2B2STAR)s.patient_mapping
        where patient_ide_source = :patient_ide_source
        and patient_ide between :bene_id_first and :bene_id_last
        ''' % dict(I2B2STAR=self.project.star_schema)

        params = dict(patient_ide_source=self.patient_ide_source,
                      bene_id_first=bene_id_first,
                      bene_id_last=bene_id_last)
        log_plan(lc, event='patient_mapping', sql=q, params=params)
        return pd.read_sql(
            q, con=lc._conn,
            params=params)


class MedparMapped(BeneMapped):
    @property
    def encounter_ide_source(self,
                             key_cols='(MEDPAR_ID)') -> str:
        source_cd = self.source.source_cd[1:-1]  # strip quotes
        return source_cd + key_cols

    def encounter_mapping(self, lc: LoggedConnection,
                          bene_id_first, bene_id_last):
        q = '''
        select encounter_ide medpar_id, encounter_num
        from %(I2B2STAR)s.encounter_mapping
        where encounter_ide_source = :encounter_ide_source
        and patient_ide between :bene_id_first and :bene_id_last
        ''' % dict(I2B2STAR=self.project.star_schema)

        params = dict(encounter_ide_source=self.encounter_ide_source,
                      bene_id_first=bene_id_first,
                      bene_id_last=bene_id_last)
        log_plan(lc, event='encounter_mapping', sql=q, params=params)
        return pd.read_sql(
            q, lc._conn, params=params)

    @classmethod
    def fmt_patient_day(cls, df):
        return df.start_date.dt.strftime('%Y-%m-%d') + ' ' + df.bene_id

    @classmethod
    def _fallback(cls, df):
        # @@TODO: replace hash with something portable between Oracle and python
        return - cls.fmt_patient_day(df).apply(hash).abs()

    def with_mapping(self, lc: LoggedConnection, data,
                     bene_id='bene_id',
                     medpar_id='medpar_id'):
        bene_id_first = data.bene_id.min()
        bene_id_last = data.bene_id.min()

        pmap = self.patient_mapping(lc, bene_id_first, bene_id_last)
        emap = self.encounter_mapping(lc, bene_id_first, bene_id_last)
        obs = data.merge(pmap, on=bene_id)
        obs = obs.merge(emap, on=medpar_id, how='left')
        obs.encounter_num = obs.encounter_num.fillna(self._fallback(obs))
        return obs


class CarrierClaims(MedparMapped):
    table_name = 'bcarrier_claims'
    key_cols = ['bene_id', 'clm_id', 'clm_from_dt', 'clm_thru_dt', 'nch_wkly_proc_dt']

    i2b2_key_cols = ['encounter_num', 'patient_num', 'start_date', 'concept_cd']
    i2b2_cols = i2b2_key_cols + ['valtype_cd', 'end_date']

    bene_id_first = IntParam()
    bene_id_last = IntParam()

    @property
    def label(self) -> str:
        return self.qualified_name

    @property
    def qualified_name(self) -> str:
        return '%s.%s' % (self.source.cms_rif, self.table_name)

    def chunks(self, lc: LoggedConnection,
               chunk_size=500000) -> pd.DataFrame:
        params = dict(bene_id_first=self.bene_id_first,
                      bene_id_last=self.bene_id_last)
        meta = self.source.table_details(lc, [self.table_name])
        t = meta.tables[self.qualified_name]
        q = sqla.select([t]).where(
            t.c.bene_id.between(self.bene_id_first, self.bene_id_last))
        log_plan(lc, event='get chunk', query=q, params=params)
        return pd.read_sql(q, lc._conn, params=params, chunksize=chunk_size)

    def column_info(self, lc: LoggedConnection,
                    text_suffixes=['_NPI', '_UPIN', '_TRIL_NUM', '_PIN_NUM'],
                    dx_marker='_dgns_'):
        meta = self.source.table_details(lc, [self.table_name])
        t = meta.tables[self.qualified_name]

        info = pd.DataFrame([dict(
                column_name=col.name,
                data_type=col.type,
                valtype_cd=col_valtype_cd(col, text_suffixes))
              for col in t.columns])
        info['is_value'] = ~ info.column_name.isin(self.key_cols)
        info.loc[~ info.is_value, 'valtype_cd'] = np.nan
        info['is_dx'] = info.column_name.str.contains(dx_marker)
        return info

    def obs_data(self, lc: LoggedConnection, upload_id: int,
                 chunk_size=500000) -> Iterator[pd.DataFrame]:
        current = pd.read_sql(sqla.select([sqla.func.current_timestamp()]), lc._conn)
        cols = self.column_info(lc)
        for data in self.chunks(lc, chunk_size=chunk_size):
            dx_data = self.dx_data(data, cols)
            mapped = self.with_mapping(lc, dx_data)
            obs_fact = self.finish_facts(mapped, upload_id=upload_id, import_date=current.iloc[0][0])
            yield obs_fact

    @classmethod
    def dx_pairs(cls, bcarrier_cols,
                 suffixes=['_cd', '_vrsn']):
        dx_cols = bcarrier_cols[bcarrier_cols.is_dx]
        dx_vrsn_cols = dx_cols[1::2].reset_index()[['column_name']]
        dx_cols = dx_cols[::2].reset_index()[['column_name']]
        dx_cols = dx_cols.merge(dx_vrsn_cols, left_index=True, right_index=True,
                                suffixes=suffixes)
        return dx_cols

    @classmethod
    def dx_data(cls, data, col_info):
        """Combine diagnosis columns i2b2 style
        """
        dx_cols = cls.dx_pairs(col_info)
        dx_data = dx_stack(data, dx_cols, cls.key_cols)
        dx_data['medpar_id'] = np.nan
        dx_data['valtype_cd'] = '@'
        dx_data['concept_cd'] = [fmt_dx_code(row.dgns_vrsn, row.dgns_cd)
                                 for _, row in dx_data.iterrows()]
        return dx_data.reset_index().rename(
            columns=dict(clm_from_dt='start_date',
                         clm_thru_dt='end_date',
                         nch_wkly_proc_dt='update_date'))

    def finish_facts(self, mapped: pd.DataFrame,
                     import_date, upload_id: int,
                     provider_id='@') -> pd.DataFrame:
        obs_fact = mapped[self.i2b2_cols].copy()
        obs_fact['provider_id'] = provider_id
        obs_fact['modifier_cd'] = self.table_name.upper()
        obs_fact['instance_num'] = mapped.index
        obs_fact['sourcesystem_cd'] = self.source.source_cd[1:-1]  # kludgy
        obs_fact['download_date'] = self.source.download_date
        obs_fact['upload_id'] = upload_id
        obs_fact['import_date'] = import_date
        return obs_fact


def col_valtype_cd(col: sqla.Column,
                   text_suffixes: List[str]=[]) -> str:
    """Determine valtype_cd based on measurement level

    TODO: return enumerated value
    """
    return (
        'n' if isinstance(col.type, sqla.types.Numeric) else
        'd' if isinstance(col.type, (sqla.types.Date, sqla.types.DateTime)) else
        't' if any(col.name.endswith(suffix.lower())
                   for suffix in text_suffixes) else
        '@'
    )


def fmt_dx_code(dgns_vrsn, dgns_cd) -> str:
    if dgns_vrsn == '10':
        return 'ICD10:' + dgns_cd  # TODO: ICD10 formatting
    # was: when dgns_vrsn = '9'
    #   but I found null dgns_vrsn e.g. one record with ADMTG_DGNS_CD = V5789
    #   so let's default to the IDC9 case
    return 'ICD9:' + dgns_cd[:3] + (
        ('.' + dgns_cd[3:]) if len(dgns_cd) > 3
        else '')


def dx_stack(data, dx_cols, key_cols):
    out = None
    for ix, pair in dx_cols.iterrows():
        dx_data = data[key_cols + [pair.column_name_vrsn.lower(),
                                   pair.column_name_cd.lower()]].set_index(key_cols)
        # icd_dgns_cd11 -> icd_dgns_cd
        dx_data.columns = ['dgns_vrsn', 'dgns_cd']
        dx_data = dx_data.dropna(subset=['dgns_cd'])
        dx_data['ix'] = ix
        dx_data['column'] = pair.column_name_cd.lower()
        if out is None:
            out = dx_data
        else:
            out = out.append(dx_data)
    return out
