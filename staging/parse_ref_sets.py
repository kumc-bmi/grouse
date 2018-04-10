from collections import OrderedDict
from numbers import Number
from datetime import datetime as datetime_T

try:
    from typing import Any, Dict, Callable, List, Tuple, Union, cast
    from pathlib import Path as Path_T  # type: ignore
    Dict, Callable, List, Tuple, Path_T
    Value = Union[str, Number, datetime_T, None]
    Workbook = Any  # kludge: no openpyxl stubs
    Cell = Any      # kludge
except ImportError:
    cast = lambda x: x  # type: ignore # noqa

from unicodecsv import writer as csv_writer  # type: ignore

from parse_fts import SQLLoader, Field, Field0, LoadScript, FileInfo


def main(argv, cwd, load_workbook):
    # type: (List[str], Path_T, Callable[[str], Workbook]) -> None
    load_script_cmds = []
    sql_data = []
    tables = []

    wb = load_workbook(argv[1])
    out = Output(cwd)
    for sheet_name in wb.get_sheet_names():
        sh = RefSheet(wb.get_sheet_by_name(sheet_name).rows, sheet_name)
        print('Processing %s' % sh.table_name)
        fields, data_rows = sh.data()

        tables.append(sh.table_name)
        sql_data.append(Field.create_table(sh.table_name, fields))

        out.save_data(sh.table_name, data_rows)
        cmd = out.save_ctl(sh.table_name, fields)
        load_script_cmds.append(cmd)

    out.write_scripts(load_script_cmds, sql_data, tables)


class Output(object):
    sqlldr_script = 'sqlldr_all_ref.sh'
    sql_create = 'oracle_create_ref.sql'
    sql_drop = 'oracle_drop_ref.sql'

    def __init__(self, dest):
        # type: (Path_T) -> None
        self.__dest = dest

    @classmethod
    def csv_file_name(cls, table_name):
        # type: (str) -> str
        return table_name + '.csv'

    def save_data(self, table_name, data_rows):
        # type: (str, List[List[Value]]) -> None
        with (self.__dest / self.csv_file_name(table_name)).open('w') as fout:
            w = csv_writer(fout)
            for row in data_rows:
                w.writerow(row)

    def save_ctl(self, table_name, fields):
        # type: (str, List[Field0]) -> str
        ctl_file = self.__dest / (table_name + '.ctl')
        csv_fn = self.csv_file_name(table_name)
        SQLLoader.save(ctl_file, table_name, fields)
        return LoadScript.sqlldr_cmd(ctl_file, csv_fn,
                                     FileInfo(csv_fn, -1, -1))

    def write_scripts(self, load_script_cmds, sql_data, tables):
        # type: (List[str], List[str], List[str]) -> None
        with (self.__dest / self.sqlldr_script).open('w') as fout:
            fout.write('\n'.join(load_script_cmds))
        with (self.__dest / self.sql_create).open('w') as fout:
            fout.write('\n\n'.join(sql_data))
        with (self.__dest / self.sql_drop).open('w') as fout:
            fout.write('\n'.join(['drop table %s;' % t for t in tables]))


class RefSheet(object):
    def __init__(self, rows, sheet_name):
        # type: (List[List[Cell]], str) -> None
        self.rows = rows
        self.sheet_name = sheet_name
        self.table_name = 'ref_' + self.sheet_name.replace(' ', '_').lower()

    def data(self):
        # type: () -> Tuple[List[Field0], List[List[Value]]]
        header = {}  # type: Dict[str, Field]
        sheet_name = self.sheet_name

        def p2size(sz):
            # type: (int) -> int
            return 1 << (sz-1).bit_length()

        def update_type(head, new_type, idx):
            # type: (str, str, int) -> None
            old_type = header[head].dtype
            if old_type and old_type != new_type:
                raise RuntimeError('%s column changed to %s! %s, %d' %
                                   (old_type, new_type, sheet_name, idx))
            header[head] = header[head]._replace(dtype=new_type)

        out_rows = []  # type: List[List[Value]]
        for idx, row in enumerate(self.rows):
            # The first few lines may be description/title
            if not header:
                if None not in [c.value for c in row]:
                    header = self.header_fields(row)
            else:
                row_to_write = []  # type: List[Value]
                for cell, head in zip(row, header.keys()):
                    if isinstance(cell.value, datetime_T):
                        update_type(head, Field.DATE, idx)
                        row_to_write.append(cell.value.strftime('%Y%m%d'))
                    elif isinstance(cell.value, Number):
                        update_type(head, Field.NUMBER, idx)
                        row_to_write.append(cell.value)
                    elif cell.value:
                        update_type(head, Field.VARCHAR2, idx)
                        header[head] = header[head]._replace(
                            width=max(header[head].width,
                                      p2size(len(cell.value) +
                                             Field.MIN_VARCHAR2_LEN)))
                        row_to_write.append(cell.value.strip()
                                            if cell.value.strip()
                                            else None)
                    else:
                        row_to_write.append(None)
                if True in [c is not None for c in row_to_write]:
                    out_rows.append(row_to_write)

        fields = [cast(Field0, f) for f in header.values()]
        return fields, out_rows

    @classmethod
    def header_fields(cls, row):
        # type: (List[Any]) -> Dict[str, Field]
        def abbr_reserved(c):
            # type: (str) -> str
            ''' Replace Oracle reserved words.
            '''
            return 'levl' if c == 'level' else c

        return OrderedDict([
            (name, Field(name=name, dtype=None, label=None,
                         width=Field.MIN_VARCHAR2_LEN))
            for cell in row
            for name in [abbr_reserved(cell.value.replace(' ', '_').lower())]])


if __name__ == '__main__':
    def _tcb():
        # type: () -> None
        from sys import argv

        from openpyxl import load_workbook  # type: ignore
        from pathlib import Path

        main(argv, cwd=Path('.'), load_workbook=load_workbook)

    _tcb()
