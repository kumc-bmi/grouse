from collections import namedtuple
import csv

import luigi

from etl_tasks import DBAccessTask


class ExploreSchema(DBAccessTask):
    schema_name = luigi.Parameter()

    def output(self):
        return CSVTarget(path=self.schema_name + '.csv')

    def run(self):
        with self._dbtarget().engine.begin() as conn:
            info = ColumnInfo.from_owner(conn, self.schema_name)
            self.output().export(ColumnInfo._fields, info)


class ColumnInfo(
        namedtuple('ColumnInfo',
                   'owner table_name column_id column_name data_type')):
    @classmethod
    def from_owner(cls, conn, owner,
                   exclude='SYS_%'):
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
        return [cls(*row) for row in rows]


class CSVTarget(luigi.local_target.LocalTarget):
    def export(self, cols, data):
        with self.open('wb') as stream:
            dest = csv.writer(stream)
            dest.writerow(cols)
            dest.writerows(data)
