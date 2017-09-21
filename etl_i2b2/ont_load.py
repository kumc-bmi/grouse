'''ont_load -- load i2b2 ontology table from CSV file
'''

from datetime import datetime
from itertools import islice
from typing import Any, Dict, List, Iterator, Optional
import logging

import luigi
import sqlalchemy as sqla
from sqlalchemy import func  # type: ignore
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.engine import Engine
from sqlalchemy.types import String, DateTime, Integer  # type: ignore

from etl_tasks import CSVTarget, DBAccessTask, UploadTask
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
