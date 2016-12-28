from contextlib import contextmanager
from re import findall, match


def main(list_dir_argv, open_rd_argv, open_wr_cwd, pjoin):
    base, files = list_dir_argv()
    for fname in files:
        if fname.endswith('.fts'):
            with open_rd_argv(pjoin(base, fname)) as fin:
                filedat = fin.read()
                parse_fts(fname, filedat)


def parse_fts(fname, filedat):
    fields = parse_fields(filedat)
    if findall('Type: Comma-Separated.*', filedat):
        parse_csv(fields)
    elif findall('Format: Fixed Column ASCII', filedat):
        parse_fixed(fields)
    else:
        raise NotImplementedError(fname)


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


def parse_csv(fields, name_idx=1, type_idx=2, width_idx=4):
    return [oracle_type(col[name_idx], col[type_idx], col[width_idx])
            for col in fields]


def parse_fixed(fields, name_idx=2, type_idx=3, start_idx=4, width_idx=5):
    return [oracle_type(col[name_idx], col[type_idx], col[width_idx])
            for col in fields]


def oracle_type(name, dtype, width, xlate=dict([
        ('CHAR', 'VARCHAR2(%(width)s)'),
        ('NUM', 'NUMBER(%(prsc)s)'),
        ('DATE', 'DATE')])):
    def make_prsc(w):
        if '.' in w:
            return '%s,%s' % tuple(w.split('.'))
        return w

    return '%s %s' % (name,  xlate[dtype] %
                      dict(width=width, prsc=make_prsc(width)))

if __name__ == '__main__':
    def _tcb():
        from os import listdir, getcwd
        from os.path import join as pjoin, abspath
        from sys import argv

        def get_input_path():
            return argv[1]

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

        main(list_dir_argv, open_rd_argv, open_wr_cwd, pjoin)

    _tcb()
