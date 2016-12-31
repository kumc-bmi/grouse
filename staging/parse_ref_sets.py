from collections import OrderedDict
from contextlib import contextmanager
from numbers import Number
from recordclass import recordclass  # requirements
from unicodecsv import writer as csv_writer  # requirements


DATE = 'DATE'
NUMBER = 'NUMBER'
VARCHAR2 = 'VARCHAR2'


def main(load_workbook_argv, open_wr_cwd, get_cli, datetime):
    def err(typ_str, sheet_name, idx):
        raise RuntimeError('%s column changed! %s, %d' %
                           (typ_str, sheet_name, idx))

    wb = load_workbook_argv(get_cli()[1])
    for sheet_name in wb.get_sheet_names():
        sh = wb.get_sheet_by_name(sheet_name)
        with open_wr_cwd(sheet_name.replace(' ', '_') + '.csv') as fout:
            w = csv_writer(fout)
            for idx, row in enumerate(sh.rows):
                if idx == 0:
                    continue  # First line is a title/description
                elif idx == 1:
                    header = OrderedDict([(cell.value, None) for cell in row])
                else:
                    for cell, head in zip(row, header.keys()):
                        if isinstance(cell.value, datetime):
                            if header[head] and header[head] != DATE:
                                err(DATE, sheet_name, idx)
                            header[head] = DATE
                        elif isinstance(cell.value, Number):
                            if header[head] and header[head] != NUMBER:
                                err(NUMBER, sheet_name, idx)
                            header[head] = NUMBER
                        else:
                            if header[head] and header[head] != VARCHAR2:
                                err(VARCHAR2, sheet_name, idx)
                            header[head] = VARCHAR2
                w.writerow([cell.value for cell in row])
            print sheet_name, header


if __name__ == '__main__':
    def _tcb():
        from datetime import datetime
        from openpyxl import load_workbook  # requirements
        from os import getcwd
        from os.path import abspath
        from sys import argv

        def get_input_path():
            return argv[1]

        def get_cli():
            return argv

        def load_workbook_argv(path):
            if get_input_path() not in path:
                raise RuntimeError('%s not in %s' % (get_input_path(), path))
            return load_workbook(path, read_only=True)

        @contextmanager
        def open_wr_cwd(path):
            cwd = abspath(getcwd())
            if cwd not in abspath(path):
                raise RuntimeError('%s not in %s' % (cwd, path))
            with open(path, 'wb') as fin:
                yield fin

        main(load_workbook_argv, open_wr_cwd, get_cli, datetime)

    _tcb()
