'''ont_load -- load i2b2 ontology table from CSV file
'''

from datetime import datetime
from itertools import islice
from typing import Any, Callable, Dict, List, Iterator, Optional, cast
import logging

from sqlalchemy import MetaData, Table, Column
from sqlalchemy import func  # type: ignore
from sqlalchemy.engine import Engine
from sqlalchemy.types import String, DateTime, Integer  # type: ignore
import luigi
import pandas as pd  # type: ignore
import sqlalchemy as sqla

from cms_pd import read_sql_step
from etl_tasks import CSVTarget, DBAccessTask, LoggedConnection, UploadTask
from param_val import StrParam, IntParam
from script_lib import Script
from sql_syntax import Environment

log = logging.getLogger(__name__)


def load(db: Engine, data: Iterator[Dict[str, str]],
         name: str, prototype: str,
         extra_colnames: List[str]=[], default_length: int=64,
         skip: Optional[int]=None,
         chunk_size: int=1000) -> None:
    schema = MetaData()
    log.info('autoloading prototype ontology table: %s', prototype)
    [proto_schema, proto_name] = (prototype.split('.', 1) if '.' in prototype
                                  else ['', prototype])
    prototype_t = Table(proto_name,
                        schema or None, autoload=True, autoload_with=db,
                        schema=proto_schema)
    columns = ([col.copy() for col in prototype_t.columns] +
               [Column(n, String(length=default_length))
                for n in extra_colnames])
    ont_t = Table(name, schema, *columns)

    if skip:
        log.info('skipping %d rows...', skip)
        [ix for ix in range(skip) if not next(data)]
        rowcount = skip
    else:
        log.info('creating: %s', name)
        ont_t.create(bind=db)
        rowcount = 0

    while 1:
        log.info('parsing %d rows after row %d...', chunk_size, rowcount)
        chunk = list(typed_record(row, ont_t)
                     for row in islice(data, 0, chunk_size))
        if not chunk:
            break
        log.info('inserting %d rows after row %d...', chunk_size, rowcount)
        db.execute(ont_t.insert(), chunk)
        rowcount += len(chunk)
    log.info('inserted %d rows into %s.', rowcount, name)


def parse_date(s: str) -> datetime:
    '''
    >>> parse_date('2015/01/01 12:00:00 AM')
    datetime.datetime(2015, 1, 1, 0, 0)
    '''
    return datetime.strptime(s, '%Y/%m/%d %I:%M:%S %p')


def typed_record(row: Dict[str, str], table: Table) -> Dict[str, Any]:
    return dict((colname,
                 parse_date(v) if v and isinstance(col.type, DateTime) else
                 int(v) if v and isinstance(col.type, Integer) else
                 # Load empty strings as null per Oracle convention
                 (v or None))
                for (colname, v) in row.items()
                for col in [table.c[colname]])


class LoadOntology(DBAccessTask):
    name = StrParam()
    prototype = StrParam()
    filename = StrParam()
    delimiter = StrParam(default=',')
    extra_cols = StrParam(default='')
    rowcount = IntParam(default=1)
    skip = IntParam(default=None)

    def requires(self) -> luigi.Task:
        return SaveOntology(filename=self.filename)

    def complete(self) -> bool:
        db = self._dbtarget().engine
        table = Table(self.name, sqla.MetaData(),
                      Column('c_fullname', sqla.String))
        if not table.exists(bind=db):
            log.info('no such table: %s', self.name)
            return False
        with self.connection() as q:
            actual = q.scalar(sqla.select([func.count(table.c.c_fullname)]))
            log.info('table %s has %d rows', self.name, actual)
            return actual >= self.rowcount  # type: ignore  # sqla

    def run(self) -> None:
        with self.input().dictreader(delimiter=self.delimiter,
                                     lowercase_fieldnames=True) as data:
            load(self._dbtarget().engine, data,
                 self.name, self.prototype,
                 skip=self.skip,
                 extra_colnames=self.extra_cols.split(','))


class SaveOntology(luigi.Task):
    filename = StrParam()

    def output(self) -> luigi.Target:
        return CSVTarget(path=self.filename)

    def requires(self) -> List[luigi.Target]:
        return []


class MetaToConcepts(UploadTask):
    script = Script.concept_dimension_fill
    ont_table_name = StrParam(  # ISSUE: enumeration?
        description="table to scan for c_tablename = 'concept_dimension' records")

    @property
    def i2b2meta(self) -> str:
        raise NotImplementedError('subclass must implement')

    @property
    def variables(self) -> Environment:
        return dict(I2B2STAR=self.project.star_schema,
                    I2B2META=self.i2b2meta,
                    ONT_TABLE_NAME=self.ont_table_name)


class MigrateRows(DBAccessTask):
    '''Migrate e.g. from an analyst's ontology to runtime i2b2 metadata.
    '''
    src = StrParam()
    dest = StrParam()
    # ListParam would be cleaner, but this avoids jenkins quoting foo.
    key_cols = StrParam()
    parallel_degree = IntParam(default=24)

    sql = """
        delete from {dest} dest
        where exists (
          select 1
          from {src} src
          where {key_constraint}
        );
        insert into {dest}
        select * from {src}
        """

    def complete(self) -> bool:
        return False

    def run(self) -> None:
        key_constraints = [
            'src.{col} = dest.{col}'.format(col=col)
            for col in self.key_cols.split(',')]
        sql = self.sql.format(
            src=self.src, dest=self.dest,
            key_constraint=' and '.join(key_constraints))
        with self.connection('migrate rows') as work:
            for st in sql.split(';\n'):
                work.execute(st)
            work.execute('commit')


def topFolders(i2b2meta, lc: LoggedConnection) -> pd.DataFrame:
    folders = read_sql_step('''
    select c_table_cd, c_hlevel, c_visualattributes, c_name, upper(c_table_name) c_table_name, c_fullname
    from {i2b2meta}.table_access ta
    where upper(ta.c_visualattributes) like '_A%'
    order by ta.c_name
    '''.format(i2b2meta=i2b2meta).strip(), lc, {}).set_index('c_table_cd')
    return folders


class ResetPatientCounts(DBAccessTask):
    i2b2meta = StrParam()

    def complete(self) -> bool:
        return False

    def run(self) -> None:
        with self.connection('resetting c_totalnum') as lc:
            for table_cd, info in topFolders(self.i2b2meta, lc).iterrows():
                lc.execute(
                    '''
                    update {i2b2meta}.{table_name} set c_totalnum = null
                    '''.strip().format(i2b2meta=self.i2b2meta,
                                       table_name=info.c_table_name))


class MetaCountPatients(DBAccessTask, luigi.WrapperTask):
    i2b2star = StrParam()
    i2b2meta = StrParam()

    def requires(self) -> List[luigi.Task]:
        with self.connection('reading table_access') as lc:
            each = topFolders(self.i2b2meta, lc)
        return [
            MetaTableCountPatients(
                i2b2star=self.i2b2star,
                i2b2meta=self.i2b2meta,
                c_table_cd=c_table_cd)
            for c_table_cd in each.index
        ]


class MetaTableCountPatients(DBAccessTask):
    i2b2star = StrParam()
    i2b2meta = StrParam()
    c_table_cd = StrParam()

    def complete(self) -> bool:
        with self.connection('any c_totalnum needed?') as lc:
            return len(self.todo(lc)) == 0

    def todo(self, lc: LoggedConnection) -> pd.DataFrame:
        desc = self.activeDescendants(lc)
        Callable  # yes, we're using it
        is_container = lambda va: va.str.upper().str.startswith('C')  # type: Callable[[pd.Series], pd.Series]
        non_containers = desc[~ is_container(desc.c_visualattributes)]
        return non_containers[non_containers.c_totalnum.isnull()]

    def activeDescendants(self, lc: LoggedConnection) -> pd.DataFrame:
        top = self.top(lc)
        desc = read_sql_step(
            '''
            select c_fullname, c_hlevel, c_visualattributes, c_totalnum, c_name, c_tooltip
            from {i2b2meta}.{meta_table} meta
            where meta.c_hlevel > :c_hlevel
              and meta.c_fullname like (:c_fullname || '%')
              and upper(meta.c_visualattributes) like '_A%'
              and m_applied_path = '@'
            order by meta.c_fullname
            '''.format(i2b2meta=self.i2b2meta,
                       meta_table=top.c_table_name).strip(),
            lc=lc,
            params=dict(c_fullname=top.c_fullname, c_hlevel=int(top.c_hlevel))).set_index('c_fullname')
        return desc

    def top(self, lc: LoggedConnection) -> pd.Series:
        return read_sql_step('''
            select c_table_cd, c_hlevel, c_visualattributes, c_name
                 , upper(c_table_name) c_table_name, c_fullname
            from {i2b2meta}.table_access ta
            where upper(ta.c_visualattributes) like '_A%'
              and ta.c_table_cd = :c_table_cd
            '''.format(i2b2meta=self.i2b2meta).strip(),
                            lc, dict(c_table_cd=self.c_table_cd)).set_index('c_table_cd').iloc[0]

    def conceptPatientCount(self, top: pd.DataFrame, c_fullname: str, lc: LoggedConnection,
                            parallel_degree: int=24) -> int:
        counts = read_sql_step(
            '''
            select /*+ parallel({degree}) */
                   c_fullname, c_hlevel, c_visualattributes, c_name
                 , case
            when upper(meta.c_visualattributes)     like 'C%'
              or upper(meta.c_visualattributes) not like '_A%'
              or lower(meta.c_tablename) <> 'concept_dimension'
              then null
            when lower(meta.c_tablename) <> 'concept_dimension'
              or lower(meta.c_operator) <> 'like'
              or lower(meta.c_facttablecolumn) <> 'concept_cd'
              then -1
            else (
                select count(distinct obs.patient_num)
                from (
                    select concept_cd
                    from {i2b2star}.concept_dimension
                    where concept_path like (meta.c_dimcode || '%')
                    ) cd
                join {i2b2star}.observation_fact obs
                  on obs.concept_cd = cd.concept_cd
            )
            end c_totalnum
            from {i2b2meta}.{table_name} meta
            where meta.c_fullname = :c_fullname
            '''.strip().format(i2b2star=self.i2b2star,
                               i2b2meta=self.i2b2meta,
                               degree=parallel_degree,
                               table_name=top.c_table_name),
            lc=lc, params=dict(c_fullname=c_fullname)).set_index('c_fullname')
        [count] = cast(List[int], counts.c_totalnum.values)
        return count

    def run(self) -> None:
        with self.connection('update patient counts in %s' % self.c_table_cd) as lc:
            top = self.top(lc)
            for c_fullname, concept in self.todo(lc).iterrows():
                count = self.conceptPatientCount(top, c_fullname, lc)
                lc.execute(
                    '''
                    update {i2b2meta}.{table_name}
                    set c_totalnum = :total
                    where c_fullname = :c_fullname
                    '''.strip().format(i2b2meta=self.i2b2meta, table_name=top.c_table_name),
                    params=dict(c_fullname=c_fullname, total=count))
                lc.execute('commit')
