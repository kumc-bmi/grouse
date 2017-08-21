
# coding: utf-8

# # Loading Medicare and Medicaid Claims data into i2b2
# 
# [CMS RIF][] docs
# 
# This notebook is on demographics.
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


# ## Demographics: MBSF_AB_SUMMARY, MAXDATA_PS

# ### Breaking work into groups by beneficiary

# In[ ]:


from cms_etl import BeneIdSurvey
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


# ## Column Info: Value Type, Level of Measurement

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


# ### MAXDATA_PS: skip custom for now

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


# `maxdata_ps` has many groups of columns with names ending in `_1`, `_2`, `_3`, and so on:

# In[ ]:


col_groups = col_data_d2[col_data_d2.column_name.str.match('.*_\d+$')]
col_groups.tail()


# In[ ]:


pd.DataFrame([dict(all_cols=len(col_data_d2),
                   cols_in_groups=len(col_groups),
                   plain_cols=len(col_data_d2) - len(col_groups))])


# In[ ]:


from cms_pd import col_valtype

def _cprop(cls, valtype_override, info: pd.DataFrame) -> pd.DataFrame:
    info['valtype_cd'] = [col_valtype(c).value for c in info.column.values]

    for cd, pat in valtype_override:
        info.valtype_cd = info.valtype_cd.where(~ info.column_name.str.match(pat), cd)
    info.loc[info.column_name.isin(cls.i2b2_map.values()), 'valtype_cd'] = np.nan

    return info.drop('column', 1)

_vo = [
    ('@', r'.*race_code_\d$'),
    ('@custom_postpone', r'.*_\d+$')
]
#dem2.column_properties(col_data_d2)
colprops_d2 = _cprop(dem2.__class__, _vo, col_data_d2)

colprops_d2.query('valtype_cd != "@custom_postpone"').sort_values(['valtype_cd', 'column_name'])


# In[ ]:


colprops_d2.dtypes


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


obs_mapped = cc.with_mapping(obs_dx, pmap, emap)
obs_mapped.columns


# In[ ]:


[col.name for col in I2B2ProjectCreate.observation_fact_columns
 if not col.nullable and col.name not in obs_mapped.columns.values]


# In[ ]:


test_run = False

if test_run:
    cc.run()


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

