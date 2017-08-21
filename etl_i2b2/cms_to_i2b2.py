
# coding: utf-8

# # Loading Medicare and Medicaid Claims data into i2b2
# 
# [CMS RIF][] docs
# 
# focus is currently on carrier claims
# 
# (demographics was done in Oracle PL/SQL)
# 
# so far, we can get data in chunks, map patients and encounters, pivot diagnoses, and insert the result into an observation_fact table (which is missing some constraints).
# 
# [CMS RIF]: https://www.resdac.org/cms-data/file-availability#research-identifiable-files

# ## Python Data Science Tools
# 
# especially [pandas](http://pandas.pydata.org/pandas-docs/)

# In[ ]:


import pandas as pd
import numpy as np
import sqlalchemy as sqla
dict(pandas=pd.__version__, numpy=np.__version__, sqlalchemy=sqla.__version__)


# ## DB Access: Luigi Config, Logging
# 
# [luigi docs](https://luigi.readthedocs.io/en/stable/)

# In[ ]:


# Passwords are expected to be in the environment.
# Prompt if it's not already there.
    
def _fix_password():
    from os import environ
    import getpass
    keyname = getpass.getuser().upper() + '_SGROUSE'
    if keyname not in environ:
        environ[keyname] = getpass.getpass()
_fix_password()


# In[ ]:


import luigi


def _reset_config(path):
    '''Reach into luigi guts and reset the config.
    
    Don't ask.'''
    cls = luigi.configuration.LuigiConfigParser
    cls._instance = None  # KLUDGE
    cls._config_paths = [path]
    return cls.instance()

_reset_config('luigi-sgrouse.cfg')
luigi.configuration.LuigiConfigParser.instance()._config_paths


# In[ ]:


import cx_ora_fix

help(cx_ora_fix)


# In[ ]:


cx_ora_fix.patch_version()

import cx_Oracle as cx
dict(cx_Oracle=cx.__version__, version_for_sqlalchemy=cx.version)


# In[ ]:


import logging

concise = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%02H:%02M:%02S')

def log_to_notebook(log,
                    formatter=concise):
    log.setLevel(logging.DEBUG)
    to_notebook = logging.StreamHandler()
    to_notebook.setFormatter(formatter)
    log.addHandler(to_notebook)
    return log


# In[ ]:


from cms_etl import CMSExtract

try:
    log.info('Already logging to notebook.')
except NameError:
    cms_rif_task = CMSExtract()
    log = log_to_notebook(logging.getLogger())

    log.info('We try to log non-trivial DB access.')

    with cms_rif_task.connection() as lc:
        lc.log.info('first bene_id')
        first_bene_id = pd.read_sql('select min(bene_id) bene_id_first from %s.%s' % (
            cms_rif_task.cms_rif, cms_rif_task.table_eg), lc._conn)

first_bene_id


# ## Carrier claims data: breaking work into groups by beneficiary
# 
# We break down work by ranges of `bene_id`:

# In[ ]:


from cms_etl import BeneIdSurvey
from cms_pd import CarrierClaimUpload

survey = BeneIdSurvey(source_table=CarrierClaimUpload.table_name)
survey.script.fname


# In[ ]:


bene_chunks = survey.results()
bene_chunks
bene_chunks = pd.DataFrame(bene_chunks, columns=bene_chunks[0].keys()).set_index('chunk_num')
bene_chunks.head()


# In[ ]:


len(bene_chunks)


# Now define a task for the first chunk of beneficiaries:

# In[ ]:


from cms_pd import CarrierClaimUpload

cc = CarrierClaimUpload(bene_id_first=bene_chunks.iloc[0].bene_id_first,
                        bene_id_last=bene_chunks.iloc[0].bene_id_last,
                        chunk_rows=bene_chunks.iloc[0].chunk_rows)
cc.account, cc.source.cms_rif, cc.project.star_schema


# Within each group, we process the claims a few thousand at a time.
# 
# _`sqlalchemy` makes a rather verbose query to get the input.
# Note that we log the execution plan as well._

# In[ ]:


with cc.connection() as lc:
    cclaims_in = next(cc.chunks(lc, chunk_size=2000))
cclaims_in.info()


# In[ ]:


cclaims_in.head()


# ## Column Info: Value Type, Level of Measurement

# In[ ]:


with cc.connection() as lc:
    bcarrier_db_cols = cc.column_data(lc)
bcarrier_db_cols.head(3).set_index('column_name')[['data_type']]


# Assign i2b2 value types based on column info:
# 
# _See also: [levels of measurement][1]._
# 
# _Diagnosis columns are discussed below._
# 
# [1]: https://en.wikipedia.org/wiki/Level_of_measurement

# In[ ]:


from cms_pd import Valtype, col_valtype

list(Valtype), [t.value for t in Valtype], 't' in Valtype, Valtype('t') in Valtype


# In[ ]:


bcarrier_cols = cc.column_properties(bcarrier_db_cols)
bcarrier_cols[~ bcarrier_cols.is_dx].sort_values('valtype_cd').set_index('column_name')


# We did get them all, right?

# In[ ]:


bcarrier_cols[~ bcarrier_cols.is_dx &
              ~ bcarrier_cols.valtype_cd.isin([t.value for t in Valtype]) &
              ~ bcarrier_cols.column_name.isin(cc.i2b2_map.values())]


# ## Observation Facts by Value Type

# ### Nominal data (no value type: @)

# In[ ]:


obs_cd = cc.pivot_valtype(Valtype.coded, cclaims_in, cc.table_name, bcarrier_cols[~ bcarrier_cols.is_dx])

(obs_cd.set_index(['bene_id', 'start_date', 'instance_num', 'modifier_cd'])
       .sort_index().head(15)[['valtype_cd', 'concept_cd']])


# ### Ordinal data (text: t)

# In[ ]:


obs_txt = cc.pivot_valtype(Valtype.text, cclaims_in, cc.table_name, bcarrier_cols[~ bcarrier_cols.is_dx])

obs_txt.set_index(['bene_id', 'start_date', 'concept_cd', 'instance_num', 'modifier_cd']
                  ).sort_index().head(10)[['valtype_cd', 'tval_char']]


# ### Interval data (date: d)

# In[ ]:


obs_dt = cc.pivot_valtype(Valtype.date, cclaims_in, cc.table_name, bcarrier_cols[~ bcarrier_cols.is_dx])

obs_dt.set_index(['bene_id', 'concept_cd', 'instance_num', 'modifier_cd']
                  ).sort_index()[::20].head()[['valtype_cd', 'tval_char', 'start_date']]


# ### Ratio data (numeric: n)

# In[ ]:


obs_num = cc.pivot_valtype(Valtype.numeric, cclaims_in, cc.table_name, bcarrier_cols[~ bcarrier_cols.is_dx])
obs_num.set_index(['bene_id', 'start_date', 'concept_cd', 'instance_num', 'modifier_cd']
                  ).sort_index().head(10)[['valtype_cd', 'nval_num']]


# All together now...

# In[ ]:


(obs_cd.append(obs_num).append(obs_txt).append(obs_dt)
 .set_index(['bene_id', 'instance_num', 'concept_cd'])  # , 'modifier_cd'
 .sort_index()
 .head(30)[
    ['start_date', 'valtype_cd', 'nval_num', 'tval_char', 'end_date', 'update_date']])


# ### Diagnoses: combining column groups

# In[ ]:


from cms_pd import fmt_dx_codes

#   I found null dgns_vrsn e.g. one record with ADMTG_DGNS_CD = V5789
#   so let's default to the IDC9 case
x = pd.DataFrame({'dgns_cd':   '185 4011 V0481 78552 R03 C220'.split() + ['V5789'],
                  'dgns_vrsn': '  9    9     9     9  10   10'.split() + [None]})


fmt_dx_codes(x.dgns_vrsn, x.dgns_cd)


# In[ ]:


from cms_pd import col_groups

dx_cols = col_groups(bcarrier_cols[bcarrier_cols.is_dx], ['_cd', '_vrsn'])
dx_cols


# In[ ]:


obs_dx = cc.dx_data(cclaims_in, cc.table_name, bcarrier_cols)
obs_dx.set_index(['bene_id', 'start_date', 'instance_num', 'modifier_cd']).sort_index().head(15)


# ## Patient, Encounter Mapping

# In[ ]:


obs_facts = obs_dx.append(obs_cd).append(obs_num).append(obs_txt).append(obs_dt)

with cc.connection('patient map') as lc:
    pmap = cc.patient_mapping(lc, (obs_facts.bene_id.min(), obs_facts.bene_id.max()))


# In[ ]:


from etl_tasks import I2B2ProjectCreate

obs_patnum = obs_facts.merge(pmap, on='bene_id')
obs_patnum.sort_values('start_date').head()[[
    col.name for col in I2B2ProjectCreate.observation_fact_columns
    if col.name in obs_patnum.columns.values]]


# In[ ]:


with cc.connection() as lc:
    emap = cc.encounter_mapping(lc, (obs_dx.bene_id.min(), obs_dx.bene_id.max()))
emap.head()


# In[ ]:


'medpar_id' in obs_patnum.columns.values


# In[ ]:


obs_pmap_emap = cc.pat_day_rollup(obs_patnum, emap)
x = obs_pmap_emap
(x[(x.encounter_num > 0) | (x.encounter_num % 8 == 0) ][::5]
  .reset_index().set_index(['patient_num', 'start_date', 'encounter_num']).sort_index()
  .head(15)[['medpar_id', 'start_day', 'admsn_dt', 'dschrg_dt', 'concept_cd']])


# ### Provider etc. done?

# In[ ]:


[col for col in I2B2ProjectCreate.observation_fact_columns
 if not col.nullable and col.name not in obs_pmap_emap.columns.values]


# ### No provider for carrier_claims???
# 
# See [missing Carrier Claim Billing NPI Number #8](https://github.com/kumc-bmi/grouse/issues/8):

# In[ ]:


'carr_clm_blg_npi_num' in bcarrier_cols.columns.values


# ## Insert Facts

# In[ ]:


clock = cc.source.download_date.__class__.now  # KLUDGE


# In[ ]:


fact1 = cc.with_admin(obs_pmap_emap, import_date=clock(), upload_id=100)
fact1.head()


# In[ ]:


with cc.connection('test write') as lc:
    fact1.head(100).to_sql(name='observation_fact_100', con=lc._conn,
                   if_exists='append', index=False)


# ## All together, from the top

# In[ ]:


with cc.connection() as lc:
    for x in cc.obs_data(lc, upload_id=100):
        break

x.head()


# In[ ]:


test_run = False

if test_run:
    cc.run()


# ### Carrier Line

# In[ ]:


from cms_pd import _DxPxCombine

class CarrierLineUpload(_DxPxCombine):
    table_name = 'bcarrier_line'

    #oops?
    #valtype_override = [
    #    ('@dx', r'.*(_dgns_|rsn_visit)'),
    #    ('@px', r'.*prcdr_')
    #]

bl = CarrierLineUpload(bene_id_first=bene_chunks.iloc[0].bene_id_first,
                 bene_id_last=bene_chunks.iloc[0].bene_id_last,
                 chunk_rows=bene_chunks.iloc[0].chunk_rows,
                 chunk_size=1000)

with bl.connection() as lc:
    bl_col_data = bl.column_data(lc)

bl_col_data.head(3)


# In[ ]:


bl_cols = bl.column_properties(bl_col_data)
bl_cols.sort_values(['valtype_cd', 'column_name'])


# ## Drugs: PDE

# In[ ]:


from cms_pd import DrugEventUpload

du = DrugEventUpload(bene_id_first=bene_chunks.iloc[0].bene_id_first,
                     bene_id_last=bene_chunks.iloc[0].bene_id_last,
                     chunk_rows=bene_chunks.iloc[0].chunk_rows,
                     chunk_size=1000)

with du.connection() as lc:
    du_cols = du.column_data(lc)


# In[ ]:


du.column_properties(du_cols).sort_values('valtype_cd')


# In[ ]:


with du.connection() as lc:
    for x, pct_in in du.obs_data(lc, upload_id=100):
        break


# In[ ]:


x.sort_values(['instance_num', 'valtype_cd']).head(50)


# ## Demographics: MBSF_AB_SUMMARY, MAXDATA_PS

# In[ ]:


from cms_pd import MBSFUpload

survey_d = BeneIdSurvey(source_table=MBSFUpload.table_name)
chunk_m0 = survey_d.results()[0]
chunk_m0 = pd.Series(chunk_m0, index=chunk_m0.keys())
chunk_m0


# In[ ]:


dem = MBSFUpload(bene_id_first=chunk_m0.bene_id_first,
                 bene_id_last=chunk_m0.bene_id_last,
                 chunk_rows=chunk_m0.chunk_rows)
dem


# In[ ]:


with dem.connection() as lc:
    col_data_d = dem.column_data(lc)
col_data_d.head(3)


# In[ ]:


colprops_d = dem.column_properties(col_data_d)
colprops_d.sort_values(['valtype_cd', 'column_name'])


# In[ ]:


with dem.connection() as lc:
    for x, pct_in in dem.obs_data(lc, upload_id=100):
        break
pct_in


# In[ ]:


x.sort_values(['instance_num', 'valtype_cd']).head(50)


# In[ ]:


from cms_pd import MAXPSUpload

survey_d = BeneIdSurvey(source_table=MAXPSUpload.table_name)
chunk_ps0 = survey_d.results()[0]
chunk_ps0 = pd.Series(chunk_ps0, index=chunk_ps0.keys())
chunk_ps0


# In[ ]:


dem2 = MAXPSUpload(bene_id_first=chunk_ps0.bene_id_first,
                  bene_id_last=chunk_ps0.bene_id_last,
                  chunk_rows=chunk_ps0.chunk_rows)
dem2


# In[ ]:


with dem2.connection() as lc:
    col_data_d2 = dem2.column_data(lc)
col_data_d2.head(3)


# In[ ]:


colprops_d2 = dem2.column_properties(col_data_d2)
x = colprops_d2.sort_values(['valtype_cd', 'column_name'])
x[~x.column_name.str.match(r'.*(_mo_|_flg_|_ind_)\d\d?$')]


# ## Outpatient Claims: Procedures (WIP)
# 
# 
# Here we deal with diagnoses as well as procedures.

# In[ ]:


from cms_pd import OutpatientClaimUpload

oc = OutpatientClaimUpload(bene_id_first=bene_chunks.iloc[0].bene_id_first,
                        bene_id_last=bene_chunks.iloc[0].bene_id_last)


# In[ ]:


with oc.connection() as lc:
    chunks = oc.chunks(lc, chunk_size=5000)
    while 1:
        oclaims_in = next(chunks)
        proc_qty = (~oclaims_in.icd_prcdr_cd1.isnull()).sum()
        print("@@found:", proc_qty)
        if proc_qty >= 4:
            break
    #x = pd.read_sql('select * from cms_deid.OUTPATIENT_BASE_CLAIMS where rownum <= 100', lc._conn)
print(len(oclaims_in))
oclaims_in.head()


# In[ ]:


with oc.connection() as lc:
    ocol_info = cc.column_properties(oc.column_data(lc))
ocol_info[ocol_info.valtype_cd.isnull()]


# In[ ]:


'bene_id' in ocol_info.column_name.values


# In[ ]:


col_groups(ocol_info[ocol_info.is_px], ['_cd', '_vrsn', '_dt'])


# In[ ]:


oclaims_in[['icd_prcdr_cd1']].drop_duplicates()


# In[ ]:


x = pd.DataFrame({'prcdr_cd':   '9904 064 99321'.split(),
                  'prcdr_vrsn': '   9   9 HCPCS'.split()})
x
# select px_code('9904', '9') from dual; -- ICD9:99.04
# select px_code('064', '9') from dual; -- ICD9:06.4
# select px_code('99321', 'HCPCS') from dual; -- CPT:99321


# In[ ]:


def fmt_px_codes(prcdr_cd: pd.Series, prcdr_vrsn: pd.Series) -> pd.Series:
    # TODO: ICDC10??
    out = np.where(prcdr_vrsn.isin(['CPT', 'HCPCS']),
                   'CPT:' + prcdr_cd,
                   'ICD9:')
    decimal_pos = np.where(prcdr_vrsn == '9', 2, np.nan)
    before = dgns_cd.str.slice(stop=decimal_pos)
    after = dgns_cd.str.slice(start=decimal_pos)
        scheme = 'ICD' + dgns_vrsn.where(~dgns_vrsn.isnull(), '9')
    decimal = np.where(dgns_cd.str.len() > decimal_pos, '.', '')
    return scheme + ':' + before + decimal + after


# In[ ]:


# select px_code('9904', '9') from dual; -- ICD9:99.04
# select px_code('064', '9') from dual; -- ICD9:06.4
# select px_code('99321', 'HCPCS') from dual; -- CPT:99321
def fmt_px_code(prcdr_cd: str, prcdr_vrsn: str) -> str:
  return (('CPT:' + prcdr_cd) if prcdr_vrsn in ['CPT', 'HCPCS'] else 
          ('ICD9:' + prcdr_cd[:2] + '.' + prcdr_cd[2:]) if prcdr_vrsn == '9' else 
          ('ICD9' + prcdr_vrsn + ':' + prcdr_cd))

fmt_px_code('9904', '9'), fmt_px_code('064', '9'), fmt_px_code('99321', 'HCPCS')


# In[ ]:


from typing import List

def px_data(data: pd.DataFrame, table_name, col_info: pd.DataFrame, ix_cols: List[str]) -> pd.DataFrame:
    """Combine procedure columns i2b2 style
    """
    px_cols = col_groups(col_info[col_info.is_px], ['_cd', '_vrsn', '_dt'])
    px_data = obs_stack(data, table_name, px_cols, ix_cols, ['prcdr_cd', 'prcdr_vrsn', 'prcdr_dt'])
    px_data['valtype_cd'] = '@'
    px_data['concept_cd'] = [fmt_px_code(row.prcdr_cd, row.prcdr_vrsn)
                             for _, row in px_data.iterrows()]
    return px_data.rename(columns=dict(prcdr_dt='start_date'))

if 0:
    px_data(oclaims_in[~oclaims_in.icd_prcdr_cd1.isnull()], oc.table_name, ocol_info, oc.ix_cols)


# In[ ]:


ocol_info[~ ocol_info.is_px  & ~ ocol_info.is_dx].sort_values('valtype_cd')


# This one is not a diagnosis code:

# In[ ]:


oclaims_in[['clm_mdcr_non_pmt_rsn_cd']].drop_duplicates()


# In[ ]:


oclaims_in[['clm_mdcl_rec']].drop_duplicates()


# Clearly `at_physn_npi` is the one to use:

# ## Performance Results

# In[ ]:


bulk_migrate = '''
insert /*+ parallel(24) append */ into dconnolly.observation_fact
select * from dconnolly.observation_fact_2440
'''


# In[ ]:


with cc.connection() as lc:
    lc.execute('truncate table my_plan_table')
    print(lc._conn.engine.url.query)
    print(pd.read_sql('select count(*) from my_plan_table', lc._conn))
    lc._conn.execute('explain plan into my_plan_table for ' + bulk_migrate)
    plan = pd.read_sql('select * from my_plan_table', lc._conn)

plan


# In[ ]:


with cc.connection() as lc:
    lc.execute('truncate table my_plan_table')
    print(pd.read_sql('select * from my_plan_table', lc._conn))
    db = lc._conn.engine
    cx = db.dialect.dbapi
    dsn = cx.makedsn(db.url.host, db.url.port, db.url.database)
    conn = cx.connect(db.url.username, db.url.password, dsn,
                      threaded=True, twophase=True)
    cur = conn.cursor()
    cur.execute('explain plan into my_plan_table for ' + bulk_migrate)
    cur.close()
    conn.commit()
    conn.close()
    plan = pd.read_sql('select * from my_plan_table', lc._conn)

plan


# In[ ]:


select /*+ parallel(24) */ max(bene_enrollmt_ref_yr)
from cms_deid.mbsf_ab_summary;


# In[ ]:


select * from upload_status
where upload_id >= 2799 -- and message is not null -- 2733
order by upload_id desc;
-- order by end_date desc;


# In[ ]:


select load_status, count(*), min(upload_id), max(upload_id), min(load_date), max(end_date)
     to_char("(sum(loaded_record),", "'999,999,999')", "loaded_record")
     round("(sum(loaded_record)", "/", "1000", "/", "((max(end_date)", "-", "min(load_date))", "*", "24", "*", "60))", "krows_min")
from (
  select upload_id, loaded_record, load_status, load_date, end_date, end_date - load_date elapsed
  from upload_status
  where upload_label like 'MBSFUp%'
)
group by load_status
("")


# ## Reimport code into running notebook

# In[ ]:


import importlib

import cms_pd
import cms_etl
import etl_tasks
import eventlog
import script_lib
importlib.reload(script_lib)
importlib.reload(eventlog)
importlib.reload(cms_pd)
importlib.reload(cms_etl)
importlib.reload(etl_tasks);

