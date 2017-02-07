''' Build Oracle DDL/Control files based on .fts input
'''
from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from csv import DictWriter
from functools import partial
from re import findall, match

import logging  # Exception to OCAP

logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
log = logging.getLogger(__name__)

FileInfo = namedtuple('FileInfo', 'filename rows bytes')


def main(list_dir_argv, open_rd_argv, open_wr_cwd, pjoin, get_cli,
         fts_extension='.fts', oracle_create='oracle_create.sql',
         oracle_drop='oracle_drop.sql', sqlldr_all='sqlldr_all.sh',
         tcdesc_file='table_column_description.csv'):

    base, files = list_dir_argv()
    table_to_ddl = dict()
    fts_file_count = 0
    fts_combine_count = 0
    load_script_data = 'set -evx\n\n'
    tcdesc = OrderedDict()
    for fname in files:
        if fname.endswith(fts_extension):
            fts_file_count += 1
            log.info('Parsing %s' % fname)
            with open_rd_argv(pjoin(base, fname)) as fin:
                tname = file_to_table_name(fname)
                log.info('Generating DDL/ctl for table %s' % tname)
                filedat = fin.read()
                ddl, mk_ctl, files, cdesc = fts_to_ddl_ctl(
                    fname, tname, filedat)
                tcdesc[tname] = cdesc

                # Make sure we don't get differing DDL for what we believe to
                # be the same table structure in the .fts files.
                if tname in table_to_ddl.keys():
                    fts_combine_count += 1
                    if table_to_ddl[tname] != ddl:
                        raise RuntimeError('Differing DDL for %s' % tname)
                table_to_ddl[tname] = ddl

                # Write out a .ctl file for every .fts file.
                ctl_file_name = fname.strip(fts_extension) + '.ctl'
                log.info('Writing %s' % ctl_file_name)
                with open_wr_cwd(ctl_file_name) as ctl_fout:
                    ctl_data = mk_ctl(fts_to_ctl_end_year(fname))
                    ctl_fout.write(ctl_data)

                if not files:
                    raise RuntimeError('No data files found for %s' % fname)

                for datafile in files:
                    load_script_data += load_script(
                        ctl_file_name,
                        pjoin(base, datafile.filename),
                        datafile.filename,
                        'The above should have loaded %s rows.' %
                        (datafile.rows if datafile.rows else
                         '(not specified in the .fts)'))

    # Write out a single file for all the create table statements.
    log.info('Writing all "create table" DDL to %s' % oracle_create)
    with open_wr_cwd(oracle_create) as fout:
        fout.write('\n\n'.join(table_to_ddl.values()))

    # Another file for all the drop table statements.
    log.info('Writing all "drop table" DDL to %s' % oracle_drop)
    with open_wr_cwd(oracle_drop) as fout:
        for tname in table_to_ddl.keys():
            fout.write('drop table %s;\n' % tname)

    # One more for the shell script to load everything
    log.info('Writing all sqlldr commands to %s' % sqlldr_all)
    with open_wr_cwd(sqlldr_all) as fout:
        fout.write(load_script_data)

    # Write out table/column descriptions
    log.info('Writing table/column descriptions to %s' % tcdesc_file)
    with open_wr_cwd(tcdesc_file) as fout:
        header = ['table_name', 'column_name', 'description']
        dw = DictWriter(fout, header)
        dw.writeheader()
        for table, cols in tcdesc.items():
            for col, desc in cols.items():
                dw.writerow(dict(zip(header, (table, col, desc))))

    log.info('Processed %d .fts files, wrote DDL for %d tables '
             '(%d .fts files were combined).' %
             (fts_file_count, len(table_to_ddl.values()), fts_combine_count))


def load_script(ctl_file_name, data_file_path, data_file, comment=''):
    return ('sqlldr $SQLLDR_USER/$SQLLDR_PASSWORD@$ORACLE_SID '
            'control=%(ctl_file_name)s '
            'direct=true rows=1000000 '
            'log=%(log_file_name)s '
            'bad=%(bad_file_name)s '
            'data=%(data_file_path)s\n\n'
            '# %(comment)s\n\n' % dict(
                ctl_file_name=ctl_file_name,
                log_file_name=data_file + '.log',
                bad_file_name=data_file + '.bad',
                data_file_path=data_file_path,
                comment=comment))


def oracle_ddl(table_name, oracle_cols,
               extra_cols=('EXTRACT_DT DATE',)):
    return ('''create table %(table_name)s (
    %(cols)s
    );''' % dict(table_name=table_name,
                 cols=',\n'.join(oracle_cols + extra_cols)))


def oracle_ctl_csv(table_name, oracle_cols, source_date_ctl):
    return '''load data
    append
    into table %(table_name)s
    fields terminated by "," optionally enclosed by '"'
    trailing nullcols
    (%(cols)s
    )''' % dict(table_name=table_name,
                cols=',\n'.join(oracle_cols + (source_date_ctl,)))


def oracle_ctl_fixed(table_name, oracle_cols, source_date_ctl):
    return '''load data
    append
    into table %(table_name)s
    (%(cols)s
    )''' % dict(table_name=table_name,
                cols=',\n'.join(oracle_cols + (source_date_ctl,)))


def file_to_table_name(fname, rev='j', skip='file'):
    '''
    >>> file_to_table_name('bcarrier_claims_j_res000050354_req005900_2011.fts')
    'bcarrier_claims'
    >>> file_to_table_name('medpar_all_file_res000050354_req005900_2011.fts')
    'medpar_all'
    >>> file_to_table_name('maxdata_ot_2011.fts')
    'maxdata_ot'
    '''
    new_parts = []
    for part in fname.split('.')[0].split('_'):
        if(part.startswith('res') or
           part == rev or
           part == skip or
           part.isdigit()):
            break
        new_parts.append(part)
    return '_'.join(new_parts)


def fts_to_ctl_end_year(fname, col_name='EXTRACT_DT'):
    '''
    >>> print fts_to_ctl_end_year(
    ... 'bcarrier_claims_j_res000050354_req005900_2011.fts')
    EXTRACT_DT "to_date('20111231', 'YYYYMMDD')"
    >>> print fts_to_ctl_end_year('maxdata_ot_2011.fts')
    EXTRACT_DT "to_date('20111231', 'YYYYMMDD')"
    '''
    m = match('.*?(_[0-9]{4})(\.fts)', fname)
    return('%(col_name)s "to_date(\'%(year)s1231\', \'YYYYMMDD\')"' %
           dict(year=m.group(1).strip('_'), col_name=col_name))


def fts_to_ddl_ctl(fname, tname, filedat):
    fields = parse_fields(filedat)
    if findall('Type: Comma-Separated.*', filedat):
        log.info('%s specifies comma-separated columns.' % fname)
        # Unzip the list of tuples [(ddl,ctl),(ddl,ctl),...]
        ddl_lines, ctl_lines = zip(*fts_csv_to_oracle_types(fields))
        mk_ctl = partial(oracle_ctl_csv, tname, ctl_lines)
        files = fts_data_files_csv(filedat)
    elif findall('Format: Fixed Column ASCII', filedat):
        log.info('%s specifies fixed-width columns.' % fname)
        ddl_lines, ctl_lines = zip(*fts_fixed_to_oracle_types(fields))
        mk_ctl = partial(oracle_ctl_fixed, tname, ctl_lines)
        files = fts_data_files_fixed(filedat)
    else:
        raise NotImplementedError(fname)
    return (oracle_ddl(tname, ddl_lines), mk_ctl, files,
            OrderedDict([(r[1], r[-1:][0]) for r in fields]))


def fts_data_files_csv(filedat):
    '''
    >>> from pkg_resources import resource_string
    >>> fts_data_files_csv(resource_string(__name__, 'sample_files_csv'))
    ... # doctest: +NORMALIZE_WHITESPACE
    [FileInfo(filename='maxdata_ia_ip_2011.csv', rows='1000', bytes='1000000'),
     FileInfo(filename='maxdata_in_ip_2011.csv', rows='2000', bytes='2000000'),
     FileInfo(filename='maxdata_ks_ip_2011.csv', rows='3000', bytes='3000000')]
    '''
    return [FileInfo(*[a.strip().replace(',', '') for a in f])
            for f in
            findall('\s+Data File:\s+(.*)?Rows:(.*)?Size\(Bytes\):(.*)',
                    filedat) if f[0].strip().endswith('.csv')]


def fts_data_files_fixed(filedat):
    '''
    >>> from pkg_resources import resource_string
    >>> fts_data_files_fixed(resource_string(__name__, 'sample_fixed'))
    ... # doctest: +NORMALIZE_WHITESPACE
    [FileInfo(filename='medpar_all_file_res000050354_req005900_2011.dat',
              rows=None, bytes=None)]

    >>> fts_data_files_fixed(resource_string(__name__, 'sample_files_fixed'))
    ... # doctest: +NORMALIZE_WHITESPACE
    [FileInfo(filename='bcarrier_line_j_res000000000_req000000_2011_001.dat',
              rows='1000000', bytes=None),
     FileInfo(filename='bcarrier_line_j_res000000000_req000000_2011_002.dat',
              rows='2000000', bytes=None),
     FileInfo(filename='bcarrier_line_j_res000000000_req000000_2011_003.dat',
              rows='3000000', bytes=None)]

    >>> fts_data_files_fixed(resource_string(__name__,
    ...                                      'sample_files_fixed_nr'))
    ... # doctest: +NORMALIZE_WHITESPACE
    [FileInfo(filename='hha_base_claims_j_res000050354_req005900_2011.dat',
              rows=None, bytes=None)]
    '''
    return [FileInfo(v[0].strip(), v[2].replace(',', '').strip()
                     if v[2] else None, None)
            for v in findall('(\w+\.dat)(\s+\(([0-9,]+) Rows\))?', filedat)]


def parse_fields(filedat):
    '''
    >>> def first_cols(rows):
    ...     return [c[:-1] for c in [r for r in rows]]
    >>> from pkg_resources import resource_string
    >>> first_cols(parse_fields(resource_string(__name__, 'sample_fixed')))
    ... # doctest: +NORMALIZE_WHITESPACE
    [['1', 'BENE_ID', 'BENE_ID', 'CHAR', '1', '15'],
    ['2', 'MEDPAR_ID', 'MEDPARID', 'CHAR', '16', '15'],
    ['3', 'MEDPAR_YR_NUM', 'MEDPAR_YR_NUM', 'CHAR', '31', '4']]

    >>> first_cols(parse_fields(resource_string(__name__, 'sample_csv')))
    ... # doctest: +NORMALIZE_WHITESPACE
    [['1', 'BENE_ID', 'CHAR', '$15.', '15'],
    ['2', 'MSIS_ID', 'CHAR', '$32.', '32'],
    ['3', 'STATE_CD', 'CHAR', '$2.', '2'], ['4', 'YR_NUM', 'NUM', '4.', '4']]
    '''
    widths = None
    rows = []
    for line in filedat.split('\n'):
        if widths and not line.strip():
            break
        elif widths:
            cols = []
            start = 0
            for width in widths:
                cols.append(line[start:start + width].strip())
                start += width + 1
            rows.append(cols)
        else:
            m = match('^-+\s+', line)
            if m:
                widths = [len(c.strip()) for c in line.split(' ') if c.strip()]
    return rows


def fts_csv_to_oracle_types(fields, name_idx=1, type_idx=2, width_idx=4):
    return [oracle_type(name=col[name_idx], dtype=col[type_idx],
                        start=None, width=col[width_idx])
            for col in fields]


def fts_fixed_to_oracle_types(fields, name_idx=1, type_idx=3,
                              start_idx=4, width_idx=5):
    return [oracle_type(name=col[name_idx], dtype=col[type_idx],
                        start=col[start_idx], width=col[width_idx])
            for col in fields]


def oracle_type(name, dtype, start, width, xlate_ddl=dict([
        ('CHAR', 'VARCHAR2(%(width)s)'),
        ('NUM', 'NUMBER'),
        ('DATE', 'DATE')]),
                ctl_date_fmt="'yyyymmdd'"):
    ddl = '%s %s' % (name,  xlate_ddl[dtype] %
                     dict(width=width))
    npos = '%s %s' % (name, 'POSITION(%d:%d)' % (
        int(start), int(start) + int(width.split('.')[0]) - 1)
                      if start else '')

    ctl = '%s %s' % (npos.strip(), "%s %s" % (dtype, ctl_date_fmt)
                     if dtype == 'DATE' else '')
    return ddl, ctl.strip()


if __name__ == '__main__':
    def _tcb():
        from os import listdir, getcwd
        from os.path import join as pjoin, abspath
        from sys import argv

        def get_input_path():
            return argv[1]

        def get_cli():
            return argv

        def list_dir_argv():
            return get_input_path(), listdir(get_input_path())

        @contextmanager
        def open_rd_argv(path):
            if get_input_path() not in path:
                raise RuntimeError('%s not in %s' % (get_input_path(), path))
            with open(path, 'rb') as fin:
                yield fin

        @contextmanager
        def open_wr_cwd(path):
            cwd = abspath(getcwd())
            if cwd not in abspath(path):
                raise RuntimeError('%s not in %s' % (cwd, path))
            with open(path, 'wb') as fin:
                yield fin

        main(list_dir_argv, open_rd_argv, open_wr_cwd, pjoin, get_cli)

    _tcb()
