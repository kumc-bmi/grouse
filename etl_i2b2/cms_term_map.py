
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
    [mbsf_abcd, ffs_claims] = [
        ('Master Beneficiary Summary - Base (A/B/C/D) Codebook',
         'https://www.ccwdata.org/documents/10280/19022436/codebook-mbsf-abcd.pdf',
         '2f7fce7c849e011d125a8487833e6cdd4ca7ced7'),
        ('Medicare Fee-For-Service Claims Codebook',
         'https://www.ccwdata.org/documents/10280/19022436/codebook-ffs-claims.pdf',
         '1e5d2e3300d8d6dab2e005ebd369d3aca02162c7')
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

def text_file(path,
              suffix='.txt'):
    df = pd.DataFrame({
        'line': [line.strip('\f\n') for line in
                 path.with_suffix('.txt').open().readlines()]})
    return df

codebook_text = text_file(mbsf_abcd_codebook)
codebook_text[:3]


# Now let's prune the TOC and page footers, leaving just the body:

# In[ ]:


def toc_dots(line,
             pattern=r'.*\.\.\.'):
    return line.str.match(pattern)

codebook_text[toc_dots(codebook_text.line)].iloc[-3:]


# In[ ]:


def footer_ix(line,
              patterns=[
        r'^\s+\^ Back to TOC \^',
        r'^CMS Chronic Conditions Data Warehouse \(CCW\)',
        r'^May 2017 – Version 1.0  \s*  Page \d+ of \d+$'],
              footer_extra=[]):
    is_footer = pd.Series(False, index=line.index)
    for footer_pattern in patterns + footer_extra:
        is_footer = is_footer | line.str.match(footer_pattern)
    return is_footer

codebook_text[footer_ix(codebook_text.line)][1:7]


# In[ ]:


def with_body(text,
              footer_extra=[]):
    is_toc = toc_dots(text.line)
    text['is_body'] = (~footer_ix(text.line, footer_extra=footer_extra) &
                       (text.index > is_toc[is_toc].index.max()))
    return text

codebook_text = with_body(codebook_text, footer_extra=[r'^Master Beneficiary Summary File \(MBSF\) with'])
codebook_text[codebook_text.is_body & (codebook_text.line > '')].head()


# There's a section for each variable:

# In[ ]:


def with_variable(text,
                  pattern=r'^   \s*([A-Z_0-9]+)$'):
    text['variable'] = text.line.str.extract(r'^   \s*([A-Z_0-9]+)$', expand=False)
    text.loc[~text.is_body, 'variable'] = None
    return text

codebook_text = with_variable(codebook_text)
codebook_text[codebook_text.line.str.strip() == codebook_text.variable].head()


# The data dictionary describes each variable and nominal value:

# In[ ]:


def extract_lhs_rhs(line):
    """
    e.g. SHORT NAME: B_MO_CNT
    also rhs with empty lhs from comment, description
    """
    df = line.str.extract(
         r'^(?:(?P<lhs>[A-Z\(\)]+(?: [A-Z]+)*):\s*|            \s*)(?P<rhs>.*)', expand=True)
    df.rhs = df.rhs.fillna('')
    return df


extract_lhs_rhs(pd.Series('''
SOURCE:        NCH
FILE(S):
VALUES:        A = Assigned claim
               N = Non-assigned claim
DESCRIPTION: The 1st diagnosis code used to identify the patient's reason for the Hospital Outpatient
visit.
'''.split('\n')))


# In[ ]:


from sys import stderr


def extract_valuesets(line):
    return line.str.extract(
        r' (?P<valueset_item>\S+) = (?P<descriptor>.*)', expand=True)


def para_agg(df, text_cols, index, columns, values,
             sep='\n'):
    return (
        df[df.lhs.isin(text_cols)]
        .groupby([index, columns])[values]
        .apply(lambda values: sep.join(txt or '' for txt in values))
        .reset_index()
        .pivot(index=index, columns=columns, values=values))


def find_pivot_dups(df, index, columns, values):
    x = df[[index, columns]].sort_values([index, columns])
    return x[x.duplicated()]


def codebook_parts(codebook_text):
    codebook = pd.concat([codebook_text,
                          extract_lhs_rhs(codebook_text.line),
                          extract_valuesets(codebook_text.line)], axis=1)

    codebook.loc[~codebook.variable.isnull(), 'lhs'] = 'variable'
    codebook.lhs = codebook.lhs.fillna(method='pad')
    codebook.variable = codebook.variable.fillna(method='pad')

    codebook = codebook[codebook.is_body & (codebook.line > '')]
    codebook_values = codebook[~codebook.valueset_item.isnull()][['variable', 'valueset_item', 'descriptor']]

    para_cols = ['LABEL', 'COMMENT', 'DESCRIPTION']
    to_pivot = codebook[~codebook.lhs.isin(
        ['variable', 'VALUES', 'CODE VALUES'] + para_cols)]
    oops = find_pivot_dups(to_pivot, index='variable', columns='lhs', values='rhs')
    if len(oops) > 0:
        print(oops, file=stderr)
        raise ValueError()
    simple_cols = to_pivot.pivot(index='variable', columns='lhs', values='rhs')
    codebook_variables = pd.concat(
        [simple_cols,
         para_agg(codebook, para_cols, 'variable', 'lhs', 'rhs')], axis=1)
    return codebook_variables, codebook_values

codebook_variables, codebook_values = codebook_parts(codebook_text)

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


# ## Medicare Fee-For-Service Claims

# In[ ]:


get_ipython().system('pdftotext -layout cache/codebook-ffs-claims.pdf')

claims_text = text_file(cms_cache[cms_cache.ffs_claims])

def fix_empty_rhs(text,
                  target_lhs='VALUES:'):
    ix = text.line[text.line == target_lhs].index.min()
    print(text.line.loc[ix:ix + 1], file=stderr)
    text.loc[ix, 'line'] = ''
    no_lhs = text.line[ix + 1]
    text.loc[ix + 1, 'line'] = target_lhs + no_lhs[len(target_lhs):]
    print(text.line.loc[ix:ix + 1], file=stderr)
    return text

claims_text = with_body(claims_text,
          footer_extra=[r'Medicare FFS Claims \(Version K\) Codebook',
                        r'^Variable Details',
                        r'^This section of the Codebook contains',
                        r'^Service Claims \(Version K\)',
                        r'^and use of the variables.'])
claims_text.line = fix_empty_rhs(claims_text)
claims_text = with_variable(claims_text)
claims_text[~claims_text.variable.isnull()].head()


# In[ ]:


claims_variables, claims_values = codebook_parts(claims_text)
claims_variables.head()


# In[ ]:


claims_values.head()


# In[ ]:


claims_variables[claims_variables['LONG NAME'] == 'prf_physn_npi'.upper()]


# In[ ]:


print(claims_variables[claims_variables['LONG NAME'] == 'prf_physn_upin'.upper()].DESCRIPTION[0])


# last update date?

# In[ ]:


for ix, v in claims_variables[claims_variables['TYPE'] == 'DATE'].iterrows():
    #print(v)
    print()
    print(v['LONG NAME'])
    print(v.DESCRIPTION)


# In[ ]:


print(claims_variables[claims_variables['LONG NAME'] == 'line_last_expns_dt'.upper()].DESCRIPTION[0])

