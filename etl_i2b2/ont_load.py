'''ont_load -- load i2b2 ontology table from CSV file
'''

from datetime import datetime
from itertools import islice
from typing import Any, Dict, List, Iterator, Optional
import logging

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.engine import Engine
from sqlalchemy.types import String, DateTime, Integer  # type: ignore

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
