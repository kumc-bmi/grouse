'''ont_load -- load i2b2 ontology table from CSV file
'''

from datetime import datetime
from itertools import islice
import csv
import logging

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.types import String, DateTime, Integer

log = logging.getLogger(__name__)


def load(db, data, name, prototype,
         extra_colnames=[], default_length=64,
         chunk_size=1000):
    schema = MetaData()
    log.info('autoloading prototype ontology table: %s', prototype)
    prototype_t = Table(prototype, schema, autoload=True, autoload_with=db)
    columns = ([col.copy() for col in prototype_t.columns] +
               [Column(n, String(length=default_length))
                for n in extra_colnames])
    ont_t = Table(name, schema, *columns)
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


def parse_date(s):
    '''
    >>> parse_date('2015/01/01 12:00:00 AM')
    datetime.datetime(2015, 1, 1, 0, 0)
    '''
    return datetime.strptime(s, '%Y/%m/%d %I:%M:%S %p')


def typed_record(row, table):
    return dict((colname,
                 parse_date(v) if isinstance(col.type, DateTime) else
                 int(v) if isinstance(col.type, Integer) else
                 v)
                for (colname, v) in row.items()
                for col in [table.c[colname]])
