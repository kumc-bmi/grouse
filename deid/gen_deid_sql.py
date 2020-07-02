''' Parse Oracle "create table" .sql file and generate sql to de-identify
CMS data.
'''
from collections import defaultdict, OrderedDict
from contextlib import contextmanager
from csv import DictReader
from re import findall, DOTALL

'''dob_cols can be picked out from table dob_col in cms_deid_mapping.sql'''
DOB_COLS = ['BENE_BIRTH_DT', 'EL_DOB', 'DOB_DT']


def main(open_rd_argv, get_input_path, get_col_desc_file, get_mode,
         print_stderr):
    print_stderr('Usage:\ngen_deid_sql.py <path/to/oracle_create.sql> '
                 '<path/to/table_column_desc.csv> '
                 '<cms_deid_sql|date_events>\n\n\n')
    print ('-- cms_deid.sql: Deidentify CMS data\n'
           '-- Copyright (c) 2020 University of Kansas Medical Center\n')

    with open_rd_argv(get_input_path()) as fin:
        tables = tables_columns(fin.read())
    with open_rd_argv(get_col_desc_file()) as fin:
        dr = DictReader(fin)
        tdesc = defaultdict(dict)
        for tcd in dr:
            tdesc[tcd['table_name']].update(
                dict([(tcd['column_name'], tcd['description'])]))
    if get_mode() == 'cms_deid_sql':
        cms_deid_sql(tables, tdesc)
    elif get_mode() == 'date_events':
        date_events(tables)
    else:
        raise NotImplementedError(get_mode())


def date_events(tables):
    def mkids(t):
        return(("'%(table)s' table_name,\n"
                '  bene_id,\n' +
                ('  msis_id,\n  state_cd,\n' if t.startswith('maxdata')
                 else '  null msis_id,\n  null state_cd,\n'))
               % dict(table=t))

    def mk_inq(t, dc):
        return ('select /*+ PARALLEL(%(table)s,12) */\n'
                '%(ids)s  %(cols)s\nfrom %(table)s'
                % dict(ids=mkids(t),
                       cols=',\n  '.join([col for col in
                                          [c2 for c2 in
                                           [("'%(col)s' COL_DT" %
                                            dict(col=dc[0])) if len(dc) == 1
                                            else ''] +
                                           [c + (' DT' if len(dc) == 1 else '')
                                            for c in dc]]
                                          if len(col.strip())]),
                       table=table))

    sql_st = list()
    for table, cols in tables.items():
        sql = ''
        date_cols = [col for (col, ctype) in cols if ctype == 'DATE']
        if len(date_cols) > 1:
            sql = ('with dates as (\n%(inq)s\n  )\n'
                   'select * from dates\nunpivot exclude nulls(\n'
                   '  dt for col_date in (\n  %(upcols)s\n))' %
                   dict(inq=mk_inq(table, date_cols),
                        upcols=',\n  '.join(["%(col)s as '%(col)s'" %
                                             dict(col=c)
                                             for c in date_cols])))
        if len(date_cols) == 1:
            sql = mk_inq(table, date_cols)

        if sql:
            sql_st.append(sql)
    print ('\n\n'.join(['insert /*+ APPEND */ into date_events\n' +
                        s + ';\ncommit;'
                        for s in sql_st]))


def cms_deid_sql(tables, tdesc, date_skip_cols=['EXTRACT_DT'],
                 hipaa_age_limit=89):
    for t_idx, (table, cols) in enumerate(tables.items()):
        sql = ('insert /*+ APPEND */ into "&&deid_schema".%(table)s\n'
               'select /*+ PARALLEL(%(table)s,12) */ \n' %
               dict(table=table, cols=',\n'.join(['  ' + c[0] for c in cols])))
        for idx, (col, ctype) in enumerate(cols):
            if ctype == 'DATE' and col not in date_skip_cols:
                if table.startswith('maxdata'):
                    dt_sql = ('  idt.%(col)s + coalesce('
                              'bm.date_shift_days, mm.date_shift_days) ' %
                              dict(col=col))
                    dob_yr = ('coalesce('
                              'extract(year from bm.birth_date_hipaa), '
                              'extract(year from mm.birth_date_hipaa))')

                    if col in DOB_COLS:
                      shift_sql = ('  case\n'
                                   '    when %(dob_yr)s = 1900\n'
                                   '    then coalesce(bm.birth_date_hipaa,mm.birth_date_hipaa)\n'
                                   '    else %(dt_sql)s\n'
                                   '  end ') % dict(col=col,
                                                    dt_sql=dt_sql.strip(),
                                                    dob_yr=dob_yr)
                    else:
                       shift_sql = dt_sql

                else:
                    dt_sql = ('  idt.%(col)s + bm.date_shift_days ' %
                              dict(col=col))
                    dob_yr= 'extract(year from bm.birth_date_hipaa)'

                    if col in DOB_COLS:
                      shift_sql = ('  case\n'
                                   '    when %(dob_yr)s = 1900\n'
                                   '    then bm.birth_date_hipaa\n'
                                   '    else %(dt_sql)s\n'
                                   '  end ') % dict(col=col,
                                                    dt_sql=dt_sql.strip(),
                                                    dob_yr=dob_yr)
                    else:
                       shift_sql = dt_sql

                sql += (shift_sql + col)

            elif col == 'BENE_ID':
                sql += ('  bm.BENE_ID_DEID %(col)s'
                        % dict(col=col))
            elif col == 'MSIS_ID':
                sql += ('  (mm.STATE_CD || mm.MSIS_ID_DEID) %(col)s'
                        % dict(col=col))
            elif (('ZIP' in col and 'PRVDR' not in col) or
                  ('COUNTY' in col) or
                  ('CNTY' in col)):
                sql += '  NULL %(col)s' % dict(col=col)
            elif col == 'BENE_AGE_AT_END_REF_YR':
                sql += ('  -- If the age at end of reference year is '
                        'null then leave it as-is. If we\'ve\n'
                        '  -- found based on some date span (such as '
                        'extract date and date of birth)\n'
                        '  -- that the age appears to be > 89 then shift '
                        'the age by the same amount we\n'
                        '  -- moved the date of birth. If that still '
                        'results in an apparent age > 89\n'
                        '  -- (presumably due to noisy data), then cap the '
                        'age at 89.\n')
                sql += ('  case\n'
                        '    when idt.%(col)s is null then null\n'
                        '    when extract(year from bm.birth_date_hipaa) = 1900 or' 
                        '         idt.%(col)s > %(hipaa_age_limit)s then %(hipaa_age_limit)s\n'
                        '    else idt.%(col)s\n'
                        '  end %(col)s') % dict(
                            col=col, hipaa_age_limit=hipaa_age_limit)
            elif col == 'BENE_AGE_CNT':
                sql += ('  case\n'
                        '    when idt.%(col)s is null then null\n'
                        '    when idt.%(col)s + round(months_between('
                        'idt.EXTRACT_DT, idt.ADMSN_DT)/12) > '
                        '%(hipaa_age_limit)s then %(hipaa_age_limit)s\n'
                        '    else idt.%(col)s\n'
                        '  end %(col)s') % dict(
                            col=col, hipaa_age_limit=hipaa_age_limit)
            else:
                sql += '  idt.%(col)s' % dict(col=col)

            if idx != len(cols) - 1:
                sql += ','
            sql += ((' -- %s\n' % tdesc[table][col])
                    if col in tdesc[table] else '\n')

        if table.startswith('maxdata'):
            sql += ('from %(table)s idt \n'
                    'left join msis_id_mapping mm '
                    'on mm.msis_id = idt.msis_id and mm.state_cd = idt.state_cd;\n'
                    'commit;\n\n') % dict(table=table)
        else:
            sql += ('from %(table)s idt \n'
                    'left join bene_id_mapping bm '
                    'on bm.bene_id = idt.bene_id;\n'
                    'commit;\n\n') % dict(table=table)

        if t_idx == len(tables) - 1:
            print sql.strip()
        else:
            print sql


def tables_columns(sql):
    '''
    >>> sql = """
    ... create table outpatient_value_codes (
    ...  BENE_ID VARCHAR2(15),
    ... CLM_VAL_AMT NUMBER
    ... );
    ...
    ... create table outpatient_base_claims (
    ...  BENE_ID VARCHAR2(15),
    ... CLM_ID VARCHAR2(15)
    ... );
    ... """
    >>> tables_columns(sql)
    ... # doctest: +NORMALIZE_WHITESPACE
    {'outpatient_value_codes': [['BENE_ID', 'VARCHAR2(15)'],
                                ['CLM_VAL_AMT', 'NUMBER']],
     'outpatient_base_claims': [['BENE_ID', 'VARCHAR2(15)'],
                                ['CLM_ID', 'VARCHAR2(15)']]}
    '''
    td = OrderedDict()
    for table, columns in findall('create table ([a-zA-Z_]+) \((.*?)\);',
                                  sql, flags=DOTALL):
        td[table] = [c.split(' ') for c in [ct.replace(',', '').strip()
                                            for ct in
                                            columns.split('\n')] if c]
    return td


if __name__ == '__main__':
    def _tcb():
        from sys import argv, stderr

        def get_input_path():
            return argv[1]

        def get_col_desc_file():
            return argv[2]

        def get_mode():
            return argv[3]

        def print_stderr(s):
            stderr.write(s)

        @contextmanager
        def open_rd_argv(path):
            if(get_input_path() not in path and
               get_col_desc_file() not in path):
                raise RuntimeError('%s not in %s' % (get_input_path(), path))
            with open(path, 'rb') as fin:
                yield fin

        main(open_rd_argv, get_input_path, get_col_desc_file, get_mode,
             print_stderr)

    _tcb()
