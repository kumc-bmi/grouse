
# coding: utf-8

# # CMS to PCORNet Terminology Mapping
# 
# The naive code building approach in `cms_pd.CMSRIFUpload.pivot_valtype` results in
# demographic codes from such as
# 
#  - `BENE_SEX_IDENT_CD:2` for Female and
#  - `BENE_RACE_CD:1` for White
# 
# The codes come from
# [Data Dictionaries - Chronic Conditions Data Warehouse](https://www.ccwdata.org/web/guest/data-dictionaries).
# 
# On the other hand, the GROUSE i2b2 uses and ontology based on
# [PCORNet CDM](http://www.pcornet.org/pcornet-common-data-model/), which uses
# 
#  - F=Female
#  - 05=White

# ## PCORNet "parseable" Common Data Model
# 
# The codes from the PCORNet are published in a nice tidy spreadsheet:

# In[ ]:


import pandas as pd
dict(pandas=pd.__version__)


# In[ ]:


from pathlib import Path
from urllib.request import build_opener as build_web_access

from cms_code_table import Cache


class PCORNetCDM(Cache):
    [v3dot1] = [
        ('PCORnet Common Data Model v3.1 Parseable Spreadsheet Format',
         'http://www.pcornet.org/wp-content/uploads/2017/01/2017-01-06-PCORnet-Common-Data-Model-v3dot1-parseable.xlsx',
         'f085b975ec5e59bef3bf505aaa3107e3f4e12e4c')
    ]
cdm_cache = PCORNetCDM.make(Path('cache'), build_web_access())

pcornet_cdm = pd.read_excel(str(cdm_cache[cdm_cache.v3dot1]), sheetname=None)

pcornet_cdm.keys()


# In[ ]:


pcornet_value = pcornet_cdm['VALUESETS'].rename(
    columns={n: n.lower() for n in pcornet_cdm['VALUESETS'].columns})
pcornet_value[pcornet_value.field_name.isin(['SEX', 'RACE'])]


# In[ ]:


pcornet_value['descriptor'] = pcornet_value.valueset_item_descriptor.str.replace('[^=]+=', '')


# ## CMS Data Dictionaries
# 
# The data dictionaries from CMS are published in PDF, so recovering the structure is a bit more involved:

# In[ ]:


from cms_code_table import Cache


class CMSDataDictionaries(Cache):
    [mbsf_abcd] = [
        ('Master Beneficiary Summary - Base (A/B/C/D) Codebook',
         'https://www.ccwdata.org/documents/10280/19022436/codebook-mbsf-abcd.pdf',
         '2f7fce7c849e011d125a8487833e6cdd4ca7ced7')
    ]
cms_cache = CMSDataDictionaries.make(Path('cache'), build_web_access())
mbsf_abcd_codebook = cms_cache[cms_cache.mbsf_abcd]
mbsf_abcd_codebook


# Then convert to text using [poppler](http://poppler.freedesktop.org/) tools...

# In[ ]:


get_ipython().system('pdftotext -layout cache/codebook-mbsf-abcd.pdf')

# Package: poppler-utils
# Original-Maintainer: Loic Minier <lool@dooz.org>
# Architecture: amd64
# Version: 0.41.0-0ubuntu1
# SHA1: 03e35d9c79455b01cffcbc635219bdb665067740
# Description-en: PDF utilities (based on Poppler)

codebook_text = pd.DataFrame({
    'line': [line.strip('\f\n') for line in
             mbsf_abcd_codebook.with_suffix('.txt').open().readlines()]})
codebook_text[:3]


# Now let's prune the TOC and page footers, leaving just the body:

# In[ ]:


toc_dots = codebook_text.line.str.match(r'.*\.\.\.')
codebook_text[toc_dots].iloc[-2:]


# In[ ]:


is_footer = pd.Series(False, index=codebook_text.index)
for footer_pattern in [
    r'^\s+\^ Back to TOC \^',
    r'^CMS Chronic Conditions Data Warehouse \(CCW\)',
    r'^Master Beneficiary Summary File \(MBSF\) with',
    r'^May 2017 – Version 1.0  \s*  Page \d+ of \d+$']:
    is_footer = is_footer | codebook_text.line.str.match(footer_pattern) 

codebook_text[is_footer][1:7]


# In[ ]:


codebook_text['is_body'] = ~is_footer & (codebook_text.index > toc_dots[toc_dots].index.max())
codebook_text[codebook_text.is_body & (codebook_text.line > '')].head()


# There's a section for each variable:

# In[ ]:


codebook_text['variable'] = codebook_text.line.str.extract(r'^   \s*([A-Z_0-9]+)$', expand=False)
codebook_text.loc[~codebook_text.is_body, 'variable'] = None
codebook_text[codebook_text.line.str.strip() == codebook_text.variable].head()


# The data dictionary describes each variable and nominal value:

# In[ ]:


def extract_lhs_rhs(line):
    """
    e.g. SHORT NAME: B_MO_CNT
    also rhs with empty lhs from comment, description
    """
    return line.str.extract(
         r'^(?:(?P<lhs>[A-Z ]+):\s+|            \s*)(?P<rhs>\S.*)', expand=True)

def extract_valuesets(line):
    return line.str.extract(
        r' (?P<valueset_item>\S+) = (?P<descriptor>.*)', expand=True)


codebook = pd.concat([codebook_text,
                      extract_lhs_rhs(codebook_text.line),
                      extract_valuesets(codebook_text.line)], axis=1)

codebook.loc[~codebook.variable.isnull(), 'lhs'] = 'variable'
codebook.lhs = codebook.lhs.fillna(method='pad')
codebook.variable = codebook.variable.fillna(method='pad')


def para_agg(df, text_cols, index, columns, values,
             sep='\n'):
    return (
        df[df.lhs.isin(text_cols)]
        .groupby([index, columns])[values]
        .apply(lambda values: sep.join(values))
        .reset_index()
        .pivot(index=index, columns=columns, values=values))

codebook = codebook[codebook.is_body & (codebook.line > '')]
codebook_values = codebook[~codebook.valueset_item.isnull()][['variable', 'valueset_item', 'descriptor']]


def find_pivot_dups(df, index, columns, values):
    x = df.groupby([index, columns])[[values]].count()
    return x[x[values] > 1]

codebook_variables = pd.concat(
    [codebook[~codebook.lhs.isin(['variable', 'COMMENT', 'DESCRIPTION', 'VALUES', 'CODE VALUES'])
             ].pivot(index='variable', columns='lhs', values='rhs'),
     para_agg(codebook, ['COMMENT', 'DESCRIPTION'], 'variable', 'lhs', 'rhs')], axis=1)
codebook_variables.head()


# In[ ]:


codebook_values[codebook_values.variable == 'SEX']


# In[ ]:


codebook_values[codebook_values.variable == 'RACE']


# ## Mapping Demographics

# In[ ]:


def map_by_descriptor(fields, codebook_values, pcornet_value):
    lhs = codebook_values.rename(columns={'variable': 'field_name'})
    rhs = pcornet_value.drop(['valueset_item_descriptor'], axis=1)
    map = pd.merge(
        lhs[codebook_values.variable.isin(fields)],
        rhs[pcornet_value.field_name.isin(fields)],
        on='descriptor', how='outer', suffixes=('_cms', '_pcornet')
    )
    return map

dem_terms_to_map = map_by_descriptor(
    ['SEX', 'RACE', 'HISPANIC'], codebook_values, pcornet_value).set_index(
    'valueset_item_order').sort_index()[['table_name', 'field_name_pcornet',
                                         'valueset_item_pcornet', 'descriptor', 'valueset_item_cms', 'field_name_cms']]
# dem_terms_to_map.to_csv('cms_pcornet_mapping.csv')
dem_terms_to_map


# In[ ]:




