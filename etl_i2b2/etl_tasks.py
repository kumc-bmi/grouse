'''etl_tasks -- Source-agnostic Luigi ETL Task support

ISSUE: This module has some i2b2 knowlege; should it be
       target-agnostic as well?

'''

import csv
from contextlib import contextmanager
from datetime import datetime

from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text as sql_text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DatabaseError
import luigi

from script_lib import Script


class DBTarget(SQLAlchemyTarget):
    '''Take advantage of engine caching logic from SQLAlchemyTarget,
    but don't bother with target_table, update_id, etc.

    >>> t = DBTarget(account='sqlite:///', passkey=None)
    >>> t.engine.scalar('select 1 + 1')
    2
    '''
    def __init__(self, account, passkey,
                 target_table=None, update_id=None,
                 echo=False):
        from os import environ  # ISSUE: ambient
        connect_args = (
            dict(password=environ[passkey]) if passkey
            else {})
        SQLAlchemyTarget.__init__(
            self,
            connection_string=account,
            target_table=target_table,
            update_id=update_id,
            connect_args=connect_args,
            echo=echo)

    def exists(self):
        raise NotImplementedError

    def touch(self):
        raise NotImplementedError


class ETLAccount(luigi.Config):
    account = luigi.Parameter()
    passkey = luigi.Parameter()


class DBAccessTask(luigi.Task):
    account = luigi.Parameter(
        default=ETLAccount().account,
        description='SQLAlchemy connection string without password')
    passkey = luigi.Parameter(
        default=ETLAccount().passkey,
        significant=False,
        description='environment variable from which to find DB password')
    echo = luigi.BoolParameter(default=False)  # TODO: proper logging

    def _dbtarget(self):
        return DBTarget(self.account, passkey=self.passkey,
                        target_table=None, update_id=self.task_id,
                        echo=self.echo)

    def output(self):
        return self._dbtarget()


class SqlScriptTask(DBAccessTask):
    '''
    >>> txform = SqlScriptTask(
    ...    account='sqlite:///', passkey=None,
    ...    script=Script.cms_patient_mapping,
    ...    variables=dict(I2B2STAR='I2B2DEMODATA', cms_source_cd='X'))

    >>> [task.script for task in txform.requires()]
    ... #doctest: +ELLIPSIS
    [<Script(i2b2_crc_design)>, <Script(cms_dem_txform)>]

    >>> txform.complete()
    False

    TODO: migrate db_util.run_script() docs for .run()
    - links to ora docs
    - whenever sqlerror continue?
    # TODO: unit test for run
    # TODO: log script_name?
    # TODO: log and time each statement? row count?
    #       structured_logging?
    #       launch/build a sub-task for each statement?
    '''
    script = luigi.EnumParameter(enum=Script)
    variables = luigi.DictParameter(default={})

    def requires(self):
        return [SqlScriptTask(script=s,
                              variables=self.variables,
                              account=self.account,
                              passkey=self.passkey,
                              echo=self.echo)
                for s in self.script.deps()]

    def complete(self):
        '''Each script's last query tells whether it is complete.

        It should be a scalar query that returns non-zero for done
        and either zero or an error for not done.
        '''
        last_query = self.script.statements(
            variables=self.variables)[-1]
        with dbtrx(self.output().engine) as tx:
            try:
                result = tx.scalar(sql_text(last_query))
                return not not result
            except DatabaseError:
                return False

    def run(self,
            bind_params={}):
        db = self.output().engine
        with dbtrx(db) as work:
            last_result = None
            fname = self.script.value[0]
            each_statement = self.script.each_statement(
                params=bind_params,
                variables=self.variables)
            for line, _comment, statement, params in each_statement:
                self.set_status_message(
                    '%s:%s: %s' % (fname, line, statement))
                try:
                    last_result = work.execute(statement, params)
                except Exception as exc:
                    raise SqlScriptError(exc, self.script, line,
                                         statement, params, str(db))
            return last_result and last_result.fetchone()

    def rollback(self):
        '''In general, the complete() method suffices and rollback() is a noop.

        See UploadTask for more.
        '''
        pass


def maybe_ora_err(exc):
    from cx_Oracle import Error as OraError
    if isinstance(exc, DatabaseError) and isinstance(exc.orig, OraError):
        return exc.orig.args[0]


class SqlScriptError(IOError):
    '''Include script file, line number in diagnostics
    '''
    def __init__(self, exc, script, line, statement,
                 params, conn_label):
        fname, _text = script.value
        message = '%s <%s>\n%s:%s:\n'
        args = [exc, conn_label, fname, line]
        ora_ex = maybe_ora_err(exc)
        if ora_ex:
            offset = ora_ex.offset
            message += '%s<ERROR>%s'
            args[0] = ora_ex.message
            args += [_pick_lines(statement[:offset], -3, None),
                     _pick_lines(statement[offset:], None, 3)]
        else:
            message = '%s'
            args += [statement]

        self.message = message
        self.args = args

    def __str__(self):
        return self.message % self.args


def _pick_lines(s, lo, hi):
    return '\n'.join(s.split('\n')[lo:hi])


class TimeStampParameter(luigi.Parameter):
    '''A datetime interchanged as milliseconds since the epoch.

    In order to get build dates from jenkins to luigi, i.e.
    from groovy to python, we use integers, since date interchange
    is a pain.
    '''

    def parse(self, s):
        ms = int(s)
        return datetime.fromtimestamp(ms / 1000.0)

    def serialize(self, dt):
        epoch = datetime.utcfromtimestamp(0)
        ms = (dt - epoch).total_seconds() * 1000
        return str(int(ms))


class _UploadTaskSupport(SqlScriptTask):
    source = luigi.TaskParameter()

    @property
    def project(self):
        return I2B2ProjectCreate()

    @property
    def transform_name(self):
        return self.script.name

    def output(self):
        return UploadTarget(self.account, self.passkey,
                            self.project.star_schema,
                            self.transform_name, self.source,
                            echo=self.echo)


class UploadTask(_UploadTaskSupport):
    def requires(self):
        return [self.project, self.source] + SqlScriptTask.requires(self)

    def complete(self):
        # Belt and suspenders
        return (self.output().exists() and
                SqlScriptTask.complete(self))

    @property
    def label(self):
        return self.script.title

    def run(self):
        upload = self.output()
        upload_id = upload.insert(label=self.label,
                                  user_id=make_url(self.account).username)
        last_result = SqlScriptTask.run(
            self,
            bind_params=dict(upload_id=upload_id,
                             download_date=self.source.download_date,
                             project_id=self.project.project_id))
        upload.update(load_status='OK', loaded_record=last_result[0])

    def rollback(self):
        script = self.script
        upload = self.output()
        tables = frozenset(
            table_name
            for dep in script.dep_closure()
            for table_name in dep.inserted_tables(self.variables))
        objects = frozenset(
            obj
            for dep in script.dep_closure()
            for obj in dep.created_objects())

        with dbtrx(self.output().engine) as work:
            upload.update(load_status=None, end_date=False)

            for table_name in tables:
                work.execute('truncate table {t}'.format(t=table_name))

            for (ty, name) in objects:
                try:
                    work.execute('drop {ty} {name}'.format(ty=ty, name=name))
                except DatabaseError:
                    pass


class UploadTarget(DBTarget):
    def __init__(self, account, passkey,
                 star_schema, transform_name, source,
                 echo=False):
        DBTarget.__init__(self, account, passkey, echo=echo)
        self.star_schema = star_schema
        self.source = source
        self.transform_name = transform_name
        self.upload_id = None

    def exists(self):
        with dbtrx(self.engine) as conn:
            exists_q = '''
            select max(upload_id) from {i2b2}.upload_status
            where transform_name = :name
            and load_status = 'OK'
            '''.format(i2b2=self.star_schema)
            upload_id = conn.scalar(sql_text(exists_q),
                                    name=self.transform_name)
            return upload_id is not None

    def insert(self, label, user_id):
        '''
        :param label: a label for related facts for audit purposes
        :param user_id: an indication of who uploaded the related facts

        ISSUE:
        :param input_file_name: path object for input file (e.g. clarity.dmp)
        '''
        with dbtrx(self.engine) as work:
            self.upload_id = work.scalar(
                sql_text(
                    '''select {i2b2}.sq_uploadstatus_uploadid.nextval
                    from dual'''.format(i2b2=self.star_schema)))

            work.execute(
                """
                insert into {i2b2}.upload_status
                (upload_id, upload_label, user_id,
                  source_cd,
                  load_date, transform_name)
                values (:upload_id, :label, :user_id,
                        :source_cd,
                        sysdate, :transform_name)
                """.format(i2b2=self.star_schema),
                upload_id=self.upload_id, label=label,
                user_id=user_id, source_cd=self.source.source_cd,
                # filename=input_file_name
                transform_name=self.transform_name)
            return self.upload_id

    def update(self, end_date=True, **args):
        '''Update SQL fields using python arguments.
        For example::

           r.update(load_status='OK')
        '''
        # TODO: Combine all this SQL conjuring with the _update_set
        #       method to increase unit test coverage.
        if self.upload_id is not None:
            key_constraint = ' where upload_id = :upload_id'
            params = dict(args, upload_id=self.upload_id)
        else:
            key_constraint = ' where transform_name = :transform_name'
            params = dict(args, transform_name=self.transform_name)

        stmt = ('update ' + self.star_schema + '.upload_status ' +
                self._update_set(**args) +
                (', end_date = sysdate' if end_date else '') +
                key_constraint)
        with dbtrx(self.engine) as work:
            work.execute(sql_text(stmt),
                         **params)

    @classmethod
    def _update_set(cls, **args):
        '''
        >>> UploadTarget._update_set(message='done', no_of_record=1234)
        'set no_of_record=:no_of_record, message=:message'
        '''
        return 'set ' + ', '.join(['%s=:%s' % (k, k) for k in args.keys()])


class I2B2ProjectCreate(DBAccessTask):
    star_schema = luigi.Parameter()  # ISSUE: use sqlalchemy meta instead?
    project_id = luigi.Parameter()

    def output(self):
        return SchemaTarget(account=self.account, passkey=self.passkey,
                            schema_name=self.star_schema,
                            table_eg='patient_dimension',
                            echo=self.echo)

    def run(self):
        raise NotImplementedError('see heron_create.create_deid_datamart etc.')


class SchemaTarget(DBTarget):
    def __init__(self, account, passkey, schema_name, table_eg,
                 echo=False):
        DBTarget.__init__(self, account, passkey, echo=echo)
        self.schema_name = schema_name
        self.table_eg = table_eg

    def exists(self):
        # ISSUE: use sqlalchemy reflection instead?
        exists_q = '''
        select 1 from {schema}.{table} where 1 = 0
        '''.format(schema=self.schema_name, table=self.table_eg)
        with dbtrx(self.engine) as conn:
            try:
                conn.execute(sql_text(exists_q))
                return True
            except DatabaseError:
                return False


@contextmanager
def dbtrx(engine):
    '''engine.being() with refined diagnostics
    '''
    try:
        conn = engine.connect()
    except DatabaseError as exc:
        raise ConnectionProblem.refine(exc, str(engine))
    with conn.begin():
        yield conn


class ConnectionProblem(DatabaseError):
    '''Provide hints about ssh tunnels.
    '''
    # connection closed, no listener
    tunnel_hint_codes = [12537, 12541]

    @classmethod
    def refine(cls, exc, conn_label):
        '''Recognize known connection problems.

        :returns: customized exception for known
                  problem else exc
        '''
        ora_ex = maybe_ora_err(exc)

        if ora_ex:
            return cls(exc, ora_ex, conn_label)
        return exc

    def __init__(self, exc, ora_ex, conn_label):
        DatabaseError.__init__(
            self,
            exc.statement, exc.params,
            exc.connection_invalidated)
        message = '%s <%s>'
        args = [ora_ex, conn_label]

        if exc.statement and ora_ex.offset:
            stmt_rest = exc.statement[
                ora_ex.offset:ora_ex.offset + 120]
            message += '\nat: %s'
            args += [stmt_rest]
        local_conn_prob = (
            ora_ex.code in self.tunnel_hint_codes and
            'localhost' in conn_label)
        if local_conn_prob:
            message += '\nhint: ssh tunnel down?'
        message += '\nin: %s'
        args += [ora_ex.context]
        self.message = message
        self.args = args

    def __str__(self):
        return self.message % self.args


class ReportTask(DBAccessTask):
    @property
    def script(self):
        raise NotImplementedError('subclass must implement')

    @property
    def report_name(self):
        raise NotImplementedError('subclass must implement')

    def complete(self):
        '''Double-check requirements as well as output.
        '''
        deps = luigi.task.flatten(self.requires())
        return (self.output().exists() and
                all(t.complete() for t in deps))

    def output(self):
        return CSVTarget(path=self.report_name + '.csv')

    def run(self):
        with dbtrx(self._dbtarget().engine) as conn:
            query = sql_text(
                'select * from {object}'.format(object=self.report_name))
            result = conn.execute(query)
            cols = result.keys()
            rows = result.fetchall()
            self.output().export(cols, rows)


class CSVTarget(luigi.local_target.LocalTarget):
    def export(self, cols, data):
        with self.open('wb') as stream:
            dest = csv.writer(stream)
            dest.writerow(cols)
            dest.writerows(data)
