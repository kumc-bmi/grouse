from collections import namedtuple
from typing import List

import luigi
from sqlalchemy.engine import Connection

from etl_tasks import DBAccessTask, CSVTarget
from param_val import StrParam


class ExploreSchema(DBAccessTask):
    schema_name = StrParam()

    def _csvout(self) -> CSVTarget:
        return CSVTarget(path=self.schema_name + '.csv')

    def output(self) -> luigi.Target:
        return self._csvout()

    def run(self) -> None:
        conn = self._dbtarget().engine.connect()
        with conn.begin():
            info = ColumnInfo.from_owner(conn, self.schema_name)
            self._csvout().export(list(ColumnInfo._fields), info)


class ColumnInfo(
        namedtuple('ColumnInfo',
                   'owner table_name column_id column_name data_type')):
    @classmethod
    def from_owner(cls, conn: Connection, owner: str,
                   exclude: str='SYS_%') -> List['ColumnInfo']:
        '''Get info on all columns in tables with a given owner.
        '''
        field_list = ', '.join(cls._fields)
        rows = conn.execute(
            '''
            select {field_list}
            from all_tab_columns
            where owner=:owner
            and table_name not like :exclude
            order by owner, table_name, column_id
            '''.format(field_list=field_list),
            owner=owner.upper(), exclude=exclude).fetchall()
        # Why can't mypy tell that RowProxy is iterable?
        return [cls(*row) for row in rows]  # type: ignore
