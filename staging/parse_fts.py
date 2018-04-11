''' Build Oracle DDL/Control files based on .fts input

We can parse an .fts input and get DDL:

    >>> fts = FTS('medpar_all_file_res000050354_req005900_2011.fts',
    ...           FTS.sample_fixed)
    >>> table_name, fields, positions, files = fts.parse()

    >>> ddl, extract_dt, combine = fts_ddl(fts, fields, {})
    >>> print ddl
    ... # doctest: +NORMALIZE_WHITESPACE
    create table medpar_all (
      BENE_ID VARCHAR2(15),
      MEDPAR_ID VARCHAR2(15),
      MEDPAR_YR_NUM VARCHAR2(4),
      EXTRACT_DT DATE
    );

and sqlldr .ctl syntax:

    >>> print SQLLoader.ctl_syntax(
    ...     table_name, fields + [extract_dt], positions)
    ... # doctest: +NORMALIZE_WHITESPACE
    load data
      append
      into table medpar_all
    (BENE_ID POSITION(1:15),
     MEDPAR_ID POSITION(16:30),
     MEDPAR_YR_NUM POSITION(31:34),
     EXTRACT_DT "to_date('20111231', 'YYYYMMDD')"
    )

along with a shell command to invoke sqlldr:

    >>> print LoadScript.sqlldr_cmd('f1.ctl',
    ...     files[0].filename, files[0])
    ... # doctest: +NORMALIZE_WHITESPACE
    sqlldr $SQLLDR_USER/$SQLLDR_PASSWORD@$ORACLE_SID
      control=f1.ctl direct=true rows=1000000
      log=medpar_all_file_res000050354_req005900_2011.dat.log
      bad=medpar_all_file_res000050354_req005900_2011.dat.bad
      data=medpar_all_file_res000050354_req005900_2011.dat
    <BLANKLINE>
    # The above should have loaded (not specified in the .fts) rows.

'''
from collections import namedtuple, OrderedDict
from csv import DictWriter
from re import findall, match
import logging  # Exception to OCAP

try:
    from typing import Callable, Dict, List, Optional as Opt, Tuple, Union
    from pathlib import Path as Path_T  # type: ignore
    Field0 = Union['Field', 'YearLiteralField']
    Dict, List, Path_T
except ImportError:
    pass

import pkg_resources as pkg  # type: ignore

log = logging.getLogger(__name__)

FileInfo = namedtuple('FileInfo', 'filename rows bytes')


class Opts(object):
    oracle_create = 'oracle_create.sql'
    oracle_drop = 'oracle_drop.sql'
    sqlldr_all = 'sqlldr_all.sh'
    tcdesc_file = 'table_column_description.csv'


def main(argv, cwd):
    # type: (List[str], Path_T) -> None
    input_path = cwd / argv[1]

    table_to_ddl = dict()   # type: Dict[str, str]
    tcdesc = OrderedDict()  # type: Dict[str, List[Field0]]
    fts_file_count, fts_combine_count = 0, 0
    load_script = []

    for ftsfile in input_path.glob('*' + FTS.extension):
        fts_file_count += 1
        log.info('Parsing %s' % ftsfile)
        with ftsfile.open() as fin:
            filedat = fin.read()
        fts = FTS(ftsfile.name, filedat)
        tname, fields, positions, files = fts.parse()
        tcdesc[tname] = fields
        ddl, extract_dt, combine = fts_ddl(fts, fields, table_to_ddl)
        fts_combine_count += combine

        ctl_file = ftsfile.with_suffix('.ctl')
        SQLLoader.save(ctl_file, tname, fields + [extract_dt], positions)

        for datafile in files:
            load_script.append(LoadScript.sqlldr_cmd(
                ctl_file, input_path / datafile.filename, datafile))

    log.info('Writing all "create table" DDL to %s' % Opts.oracle_create)
        fout.write('\n\n'.join(table_to_ddl.values()))
    with (cwd / Opts.oracle_create).open('wb') as fout:

    log.info('Writing all "drop table" DDL to %s' % Opts.oracle_drop)
        for tname in table_to_ddl.keys():
    with (cwd / Opts.oracle_drop).open('wb') as fout:
            fout.write('drop table %s;\n' % tname)

    LoadScript.save(cwd / Opts.sqlldr_all, load_script)

    Field.save_descriptions(cwd / Opts.tcdesc_file, tcdesc)

    log.info('Processed %d .fts files, wrote DDL for %d tables '
             '(%d .fts files were combined).' %
             (fts_file_count, len(table_to_ddl.values()), fts_combine_count))


def fts_ddl(fts, fields, table_to_ddl):
    # type: (FTS, List[Field0], Dict[str, str]) -> Tuple[str, Field0, int]
    '''Generate, check DDL from fts fields.
    '''
    tname = fts.table_name()
    log.info('Generating DDL/ctl for table %s' % tname)
    extract_field = YearLiteralField(
        name='EXTRACT_DT', year=fts.end_year)
    ddl = Field.create_table(tname, fields + [extract_field])

    # Make sure we don't get differing DDL for what we believe to
    # be the same table structure in the .fts files.
    combine = 0
    if tname in table_to_ddl.keys():
        combine = 1
        if table_to_ddl[tname] != ddl:
            raise RuntimeError('Differing DDL for %s' % tname)
    else:
        table_to_ddl[tname] = ddl
    return ddl, extract_field, combine


class LoadScript(object):
    @classmethod
    def save(cls, dest, commands):
        # type: (Path_T, List[str]) -> None
        log.info('Writing all sqlldr commands to %s' % dest)
        with dest.open('wb') as fout:
            fout.write('set -evx\n\n')
            for cmd in commands:
                fout.write(cmd)

    @classmethod
    def sqlldr_cmd(cls, ctl_file, data_file_path, datafile):
        # type: (Path_T, str, FileInfo) -> str
        data_file = datafile.filename
        comment = ('The above should have loaded %s rows.' %
                   (datafile.rows if datafile.rows else
                    '(not specified in the .fts)'))
        return ('sqlldr $SQLLDR_USER/$SQLLDR_PASSWORD@$ORACLE_SID '
                'control=%(ctl_file_name)s '
                'direct=true rows=1000000 '
                'log=%(log_file_name)s '
                'bad=%(bad_file_name)s '
                'data=%(data_file_path)s\n\n'
                '# %(comment)s\n\n' % dict(
                    ctl_file_name=ctl_file,
                    log_file_name=data_file + '.log',
                    bad_file_name=data_file + '.bad',
                    data_file_path=data_file_path,
                    comment=comment))


class SQLLoader(object):
    @classmethod
    def save(cls, ctl_file, table_name, fields, positions=None):
        # type: (Path_T, str, List[Field0], Opt[List[str]]) -> None
        log.info('Writing %s' % ctl_file)
        with ctl_file.open('wb') as fout:
            fout.write(cls.ctl_syntax(table_name, fields, positions))

    ctl_top = '''load data
    append
    into table %(table_name)s%(fmt_info)s
    (%(cols)s
    )'''

    @classmethod
    def ctl_syntax(cls, table_name, fields, positions=None):
        # type: (str, List[Field0], Opt[List[str]]) -> str
        if positions:
            assert len(fields) <= len(positions) + 1
            oracle_cols = [field.ctl(pos)
                           for field, pos in zip(fields, positions + [''])]
            fmt_info = ''
        else:
            oracle_cols = [field.ctl() for field in fields]
            fmt_info = '''
            fields terminated by "," optionally enclosed by '"'
            trailing nullcols
            '''

        return cls.ctl_top % dict(table_name=table_name, fmt_info=fmt_info,
                                  cols=',\n'.join(oracle_cols))


class FTS(namedtuple('FTS', 'fname filedat')):
    extension = '.fts'

    def parse(self):
        # type: () -> Tuple[str, List[Field0], Opt[List[str]], List[FileInfo]]
        fields, positions = self.parse_fields()
        if positions:
            files = FTS.data_files_fixed(self.filedat)
        else:
            files = FTS.data_files_csv(self.filedat)

        if not files:
            raise RuntimeError('No data files found for %s' % self.fname)

        return self.table_name(), fields, positions, files

    def table_name(self,
                   rev='j', skip='file'):
        # type: (str, str) -> str
        '''
        >>> fn = 'bcarrier_claims_j_res000050354_req005900_2011.fts'
        >>> FTS(fn, '').table_name()
        'bcarrier_claims'

        >>> fn = 'medpar_all_file_res000050354_req005900_2011.fts'
        >>> FTS(fn, '').table_name()
        'medpar_all'

        >>> FTS('maxdata_ot_2011.fts', '').table_name()
        'maxdata_ot'
        '''
        new_parts = []  # type: List[str]
        fname = self.fname
        for part in fname.split('.')[0].split('_'):
            if(part.startswith('res') or
               part == rev or
               part == skip or
               part.isdigit()):
                break
            new_parts.append(part)
        return '_'.join(new_parts)

    @property
    def end_year(self):
        # type: () -> int
        '''
        >>> fn = 'bcarrier_claims_j_res000050354_req005900_2011.fts'
        >>> FTS(fn, None).end_year
        2011

        >>> FTS('maxdata_ot_2011.fts', None).end_year
        2011
        '''
        m = match('.*?(_[0-9]{4})(\.fts)', self.fname)
        if not m:
            raise KeyError(self.fname)
        return int(m.group(1).strip('_'))

    def parse_fields(self):
        # type: () -> Tuple[List[Field0], Opt[List[str]]]
        fname, filedat = self.fname, self.filedat
        field_parts = FTS._parse_parts(filedat)
        try:
            start_idx, builder = self._csv_indexes()
            log.info('%s specifies comma-separated columns.' % fname)
        except KeyError:
            try:
                start_idx, builder = self._fixed_indexes(field_parts[0])
            except KeyError:
                raise NotImplementedError(fname)

        positions = [parts[start_idx] for parts in field_parts] if start_idx else None
        fields = [builder(row) for row in field_parts]
        return fields, positions

    def _csv_indexes(self):
        # type: () -> Tuple[Opt[int], Callable[[List[str]], Field0]]
        hits = findall('(Type|Format): Comma-Separated.*', self.filedat)
        if not hits:
            raise KeyError
        return None, lambda row: Field(name=row[1],
                                       dtype=row[2],
                                       width=row[4],
                                       label=row[-1])

    def _fixed_indexes(self, example_row):
        # type: (List[str]) -> Tuple[int, Callable[[List[str]], Field0]]
        ''' Field indexes containing required data fields

        We could parse out the headers from the table in the .fts, but CMS
        uses fixed widths and the headers span multiple lines.  Since we've
        only run across a couple different formats, just stick with field
        indexes for now.

        @@Mismatch with test data?
        '''
        hits = findall('Format: Fixed Column ASCII', self.filedat)
        if not hits:
            raise KeyError

        if len(example_row) == 7:
            # Sometimes the .fts files include a "Field Short Name"
            return 4, lambda row: Field(name=row[1],
                                        dtype=row[3],
                                        #@@check. was 6
                                        width=row[5],
                                        label=row[-1])
        elif len(example_row) == 6:
            # Other times, they only have a "Field Long Name"
            return 3, lambda row: Field(name=row[1],
                                        dtype=row[2],
                                        width=row[4],
                                        label=row[-1])
        else:
            raise NotImplementedError('Unexpected fixed field count!')

    sample_files_csv = pkg.resource_string(__name__, 'sample_files_csv')
    sample_csv_af = pkg.resource_string(__name__, 'sample_csv_af')

    @staticmethod
    def data_files_csv(filedat):
        # type: (str) -> List[FileInfo]
        '''
        >>> FTS.data_files_csv(FTS.sample_files_csv)
        ... # doctest: +NORMALIZE_WHITESPACE
        [FileInfo(filename='maxdata_ia_ip_2011.csv',
                  rows='1000', bytes='1000000'),
         FileInfo(filename='maxdata_in_ip_2011.csv',
                  rows='2000', bytes='2000000'),
         FileInfo(filename='maxdata_ks_ip_2011.csv',
                  rows='3000', bytes='3000000')]

        Sometimes, there's only a single file specified
        >>> FTS.data_files_csv(FTS.sample_csv_af)
        ... # doctest: +NORMALIZE_WHITESPACE
        [FileInfo(filename='unique_msis_xwalk_2011.csv', rows='100',
          bytes='1000000')]
        '''
        finfos = [findall('Actual File Name: (.*)', filedat) +
                  findall('Exact File Quantity \(Rows\): (.*)', filedat) +
                  findall('Exact File Size in Bytes with 512 Blocksize: (.*)',
                          filedat), ]
        if not finfos[0]:
            finfos = findall(
                '\s+Data File:\s+(.*)?Rows:(.*)?Size\(Bytes\):(.*)',
                filedat)
        return [FileInfo(*[a.strip().replace(',', '') for a in f])
                for f in finfos if f[0].strip().endswith('.csv')]

    sample_csv = pkg.resource_string(__name__, 'sample_csv')
    sample_fixed = pkg.resource_string(__name__, 'sample_fixed')
    sample_files_fixed = pkg.resource_string(__name__, 'sample_files_fixed')
    sample_fixed_nr = pkg.resource_string(__name__, 'sample_files_fixed_nr')

    @staticmethod
    def data_files_fixed(filedat):
        # type: (str) -> List[FileInfo]
        '''
        >>> FTS.data_files_fixed(FTS.sample_fixed)
        ... # doctest: +NORMALIZE_WHITESPACE
        [FileInfo(filename='medpar_all_file_res000050354_req005900_2011.dat',
                  rows=None, bytes=None)]

        >>> FTS.data_files_fixed(FTS.sample_files_fixed)
        ... # doctest: +NORMALIZE_WHITESPACE
        [FileInfo(filename='bcarrier_line_j_res000000000_req000000_2011_001.dat',
                  rows='1000000', bytes=None),
         FileInfo(filename='bcarrier_line_j_res000000000_req000000_2011_002.dat',
                  rows='2000000', bytes=None),
         FileInfo(filename='bcarrier_line_j_res000000000_req000000_2011_003.dat',
                  rows='3000000', bytes=None)]

        >>> FTS.data_files_fixed(FTS.sample_fixed_nr)
        ... # doctest: +NORMALIZE_WHITESPACE
        [FileInfo(filename='hha_base_claims_j_res000050354_req005900_2011.dat',
                  rows=None, bytes=None)]
        '''
        return [FileInfo(v[0].strip(), v[2].replace(',', '').strip()
                         if v[2] else None, None)
                for v in findall(
                        '(\w+\.dat)(\s+\(([0-9,]+) Rows\))?', filedat)]

    @staticmethod
    def _parse_parts(filedat):
        # type: (str) -> List[List[str]]
        '''
        >>> def first_cols(rows):
        ...     return [c[:-1] for c in [r for r in rows]]
        >>> first_cols(FTS._parse_parts(FTS.sample_fixed))
        ... # doctest: +NORMALIZE_WHITESPACE
        [['1', 'BENE_ID', 'BENE_ID', 'CHAR', '1', '15'],
         ['2', 'MEDPAR_ID', 'MEDPARID', 'CHAR', '16', '15'],
         ['3', 'MEDPAR_YR_NUM', 'MEDPAR_YR_NUM', 'CHAR', '31', '4']]

        >>> first_cols(FTS._parse_parts(FTS.sample_csv))
        ... # doctest: +NORMALIZE_WHITESPACE
        [['1', 'BENE_ID', 'CHAR', '$15.', '15'],
         ['2', 'MSIS_ID', 'CHAR', '$32.', '32'],
         ['3', 'STATE_CD', 'CHAR', '$2.', '2'],
         ['4', 'YR_NUM', 'NUM', '4.', '4']]
        '''
        widths = None
        rows = []  # type: List[List[str]]
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
                    widths = [len(c.strip()) for c in line.split(' ')
                              if c.strip()]
        return rows


class Field(namedtuple('Field', 'name dtype width label')):
    DATE = 'DATE'
    NUMBER = 'NUMBER'
    VARCHAR2 = 'VARCHAR2'
    MIN_VARCHAR2_LEN = 8

    _sql_type_map = {
        'CHAR': VARCHAR2 + '(%(width)s)',
        'NUM': NUMBER,
        'DATE': DATE,
    }

    @property
    def sql_type(self):
        # type: () -> str
        return self._sql_type_map[self.dtype]

    @property
    def ddl(self):
        # type: () -> str
        return '%s %s' % (self.name,
                          self.sql_type % dict(width=self.width))

    @staticmethod
    def create_table(table_name, fields):
        # type: (str, List[Field0]) -> str
        return ('''create table %(table_name)s (
    %(cols)s
    );''' % dict(table_name=table_name,
                 cols=',\n'.join(f.ddl for f in fields)))

    ctl_date_fmt = "DATE 'yyyymmdd'"

    def ctl(self, start_txt=None):
        # type: (Opt[str]) -> str
        dtype = self.ctl_date_fmt if self.dtype == 'DATE' else ''
        if not start_txt:
            return ('%s %s' % (self.name, dtype)).strip()

        start = int(start_txt)
        end = int(start) + int(self.width.split('.')[0]) - 1
        return ('%s POSITION(%d:%d) %s' % (
            self.name, start, end, dtype)).strip()

    @classmethod
    def save_descriptions(cls, dest, tcdesc):
        # type: (Path_T, Dict[str, List[Field0]]) -> None
        log.info('Writing table/column descriptions to %s' % dest.name)
        header = ['table_name', 'column_name', 'description']
        with dest.open('wb') as fout:
            dw = DictWriter(fout, header)
            dw.writeheader()
            for table, cols in tcdesc.items():
                for f in cols:
                    dw.writerow(dict(zip(header, (table, f.name, f.label))))


class YearLiteralField(namedtuple('YearLiteralField', 'name year')):
    '''
    >>> print YearLiteralField('EXTRACT_DT', 2011).ctl()
    EXTRACT_DT "to_date('20111231', 'YYYYMMDD')"
    '''

    @property
    def sql_type(self):
        # type: () -> str
        return 'DATE'

    @property
    def ddl(self):
        # type: () -> str
        return '%s DATE' % self.name

    def ctl(self, _pos=None):
        # type: (Opt[str]) -> str
        return('%(name)s "to_date(\'%(year)s1231\', \'YYYYMMDD\')"' %
               dict(name=self.name, year=self.year))

    @property
    def label(self):
        # type: () -> str
        return 'Last date of year of extraction'


if __name__ == '__main__':
    def _script():
        # type: () -> None
        from pathlib import Path
        from sys import argv

        logging.basicConfig(format='%(asctime)s (%(levelname)s) %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

        main(argv, cwd=Path('.'))

    _script()
