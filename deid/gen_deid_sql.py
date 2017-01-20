''' Parse Oracle "create table" .sql file and generate sql to de-identify
CMS data.
'''
from collections import defaultdict
from contextlib import contextmanager
from csv import DictReader
from re import findall, DOTALL


def main(open_rd_argv, get_input_path, get_col_desc_file, get_mode):
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
            sql = ('select * from (\nwith dates as (\n%(inq)s\n  )\n'
                   'select * from dates\nunpivot exclude nulls(\n'
                   '  dt for col_date in (\n  %(upcols)s\n)))' %
                   dict(inq=mk_inq(table, date_cols),
                        upcols=',\n  '.join(["%(col)s as '%(col)s'" %
                                             dict(col=c)
                                             for c in date_cols])))
        if len(date_cols) == 1:
            sql = mk_inq(table, date_cols)

        if sql:
            sql_st.append(sql)
    print ('insert /*+ APPEND */ into date_events\n' +
           '\n\nunion all\n\n'.join(sql_st) + '\n;\n' +
           'commit;')


def cms_deid_sql(tables, tdesc):
    for table, cols in tables.items():
        sql = ('insert /*+ APPEND */ into "&&deid_schema".%(table)s\n'
               'select /*+ PARALLEL(%(table)s,12) */ \n' %
               dict(table=table, cols=',\n'.join(['  ' + c[0] for c in cols])))
        for idx, (col, ctype) in enumerate(cols):
            if ctype == 'DATE':
                if table.startswith('maxdata'):
                    sql += ('  idt.%(col)s + coalesce('
                            'bm.date_shift_days, mp.date_shift_days) %(col)s '
                            % dict(col=col))
                else:
                    sql += ('  idt.%(col)s + bm.date_shift_days %(col)s'
                            % dict(col=col))
            elif col == 'BENE_ID':
                sql += ('  bm.BENE_ID_DEID %(col)s'
                        % dict(col=col))
            elif col == 'MSIS_ID':
                sql += ('  mm.MSIS_ID_DEID %(col)s'
                        % dict(col=col))
            elif (('ZIP' in col and 'PRVDR' not in col) or
                  ('COUNTY' in col) or
                  ('CNTY' in col)):
                sql += '  NULL %(col)s' % dict(col=col)
            else:
                sql += '  idt.%(col)s' % dict(col=col)

            if idx != len(cols) - 1:
                sql += ','
            sql += ' -- %s\n' % tdesc[table][col]

        if table.startswith('maxdata'):
            sql += ('from %(table)s idt \n'
                    'left join bene_id_mapping bm '
                    'on bm.bene_id = idt.bene_id\n'
                    'join msis_id_mapping mm '
                    'on mm.msis_id = idt.msis_id\n'
                    'join msis_person mp on mp.msis_id = idt.msis_id '
                    'and mp.state_cd = idt.state_cd;\n'
                    'commit;\n\n') % dict(table=table)
        else:
            sql += ('from %(table)s idt \n'
                    'join bene_id_mapping bm '
                    'on bm.bene_id = idt.bene_id;\n'
                    'commit;\n\n') % dict(table=table)

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
    td = dict()
    for table, columns in findall('create table ([a-zA-Z_]+) \((.*?)\);',
                                  sql, flags=DOTALL):
        td[table] = [c.split(' ') for c in [ct.replace(',', '').strip()
                                            for ct in
                                            columns.split('\n')] if c]
    return td


if __name__ == '__main__':
    def _tcb():
        from sys import argv

        def get_input_path():
            return argv[1]

        def get_col_desc_file():
            return argv[2]

        def get_mode():
            return argv[3]

        @contextmanager
        def open_rd_argv(path):
            if(get_input_path() not in path and
               get_col_desc_file() not in path):
                raise RuntimeError('%s not in %s' % (get_input_path(), path))
            with open(path, 'rb') as fin:
                yield fin

        main(open_rd_argv, get_input_path, get_col_desc_file, get_mode)

    _tcb()
