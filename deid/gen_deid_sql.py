''' Parse Oracle "create table" .sql file and generate sql to de-identify
CMS data.
'''
from contextlib import contextmanager
from re import findall, DOTALL


def main(open_rd_argv, get_input_path):
    with open_rd_argv(get_input_path()) as fin:
        tables = tables_columns(fin.read())
    for table, cols in tables.items():
        sql = ('insert into "&&deid_schema".%(table)s\n'
               'select /*+ PARALLEL(%(table)s,12) */ \n' %
               dict(table=table, cols=',\n'.join(['  ' + c[0] for c in cols])))
        for idx, (col, ctype) in enumerate(cols):
            if ctype == 'DATE':
                sql += ('  idt.%(col)s + bm.date_shift_days %(col)s'
                        % dict(col=col))
            elif col == 'BENE_ID':
                sql += ('  bm.BENE_ID_DEID %(col)s'
                        % dict(col=col))
            elif (('ZIP' in col and 'PRVDR' not in col) or
                  ('COUNTY' in col) or
                  ('CNTY' in col)):
                sql += '  NULL %(col)s' % dict(col=col)
            else:
                sql += '  idt.%(col)s' % dict(col=col)

            if idx != len(cols) - 1:
                sql += ','
            sql += '\n'

        sql += ('from %(table)s idt \n'
                'join bene_id_mapping bm '
                'on bm.bene_id = idt.bene_id;\ncommit;\n\n')
        print sql % dict(table=table)


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

        @contextmanager
        def open_rd_argv(path):
            if get_input_path() not in path:
                raise RuntimeError('%s not in %s' % (get_input_path(), path))
            with open(path, 'rb') as fin:
                yield fin

        main(open_rd_argv, get_input_path)

    _tcb()
