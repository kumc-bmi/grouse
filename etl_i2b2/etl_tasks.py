import csv

from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text as sql_text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DatabaseError
import luigi

import sql_syntax
from script_lib import Script, I2B2STAR


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


class DBAccessTask(luigi.Task):
    account = luigi.Parameter(
        description='SQLAlchemy connection string without password')
    passkey = luigi.Parameter(
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
    ...    variables=dict(I2B2STAR='I2B2DEMODATA'))

    >>> [task.script for task in txform.requires()]
    ... #doctest: +ELLIPSIS
    [<Script(i2b2_crc_design)>, <Script(cms_dem_txform)>, ...]

    >>> txform.complete()
    False

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
        db = self.output().engine
        try:
            return not not db.scalar(sql_text(last_query))
        except DatabaseError:
            return False

    def run(self,
            bind_params={}):
        # TODO: unit test for run
        # TODO: log script_name?
        with self.output().engine.begin() as work:
            last_result = None
            for statement in self.script.statements(
                    variables=self.variables):
                # TODO: log and time each statement? row count?
                # structured_logging?
                # launch/build a sub-task for each statement?
                self.set_status_message(statement)
                last_result = work.execute(
                    statement,
                    sql_syntax.params_of(statement, bind_params))
            return last_result and last_result.fetchone()


class _UploadTaskSupport(SqlScriptTask):
    source_cd = luigi.Parameter()  # ISSUE: design-time enum of sources?
    project_id = luigi.Parameter()

    def output(self):
        return UploadTarget(self.account, self.passkey,
                            self.variables[I2B2STAR],
                            self.script.name,
                            echo=self.echo)


class UploadTask(_UploadTaskSupport):
    source_cd = luigi.Parameter()  # ISSUE: design-time enum of sources?
    project_id = luigi.Parameter()

    def requires(self):
        project = I2B2ProjectCreate(account=self.account,
                                    passkey=self.passkey,
                                    star_schema=self.variables[I2B2STAR],
                                    project_id=self.project_id)
        return [project] + SqlScriptTask.requires(self)

    def run(self):
        upload = self.output()
        upload_id = upload.insert(label=self.script.title(),
                                  user_id=make_url(self.account).username,
                                  source_cd=self.source_cd)
        last_result = SqlScriptTask.run(
            self,
            bind_params=dict(upload_id=upload_id,
                             project_id=self.project_id))
        upload.update(load_status='OK', loaded_record=last_result[0])


class UploadRollback(_UploadTaskSupport):
    def complete(self):
        return False

    def run(self):
        self.rollback()

    def rollback(self):
        script = self.script
        upload = self.output()
        tables = script.inserted_tables(self.variables)
        objects = script.created_objects()

        with self.output().engine.begin() as work:
            upload.update(load_status=None, end_date=False)

            for _dep, table_name in tables:
                work.execute('truncate table {t}'.format(t=table_name))

            for (_s, (ty, name)) in objects:
                try:
                    work.execute('drop {ty} {name}'.format(ty=ty, name=name))
                except DatabaseError:
                    pass


class UploadTarget(DBTarget):
    def __init__(self, account, passkey, star_schema, transform_name,
                 echo=False):
        DBTarget.__init__(self, account, passkey, echo=echo)
        self.star_schema = star_schema
        self.transform_name = transform_name
        self.upload_id = None

    def exists(self):
        with self.engine.begin() as conn:
            exists_q = '''
            select max(upload_id) from {i2b2}.upload_status
            where transform_name = :name
            and load_status = 'OK'
            '''.format(i2b2=self.star_schema)
            upload_id = conn.scalar(sql_text(exists_q),
                                    name=self.transform_name)
            return upload_id is not None

    def insert(self, label, user_id, source_cd):
        '''
        :param label: a label for related facts for audit purposes
        :param user_id: an indication of who uploaded the related facts
        :param source_cd: value for upload_status.source_cd

        ISSUE:
        :param input_file_name: path object for input file (e.g. clarity.dmp)
        '''
        with self.engine.begin() as work:
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
                user_id=user_id, source_cd=source_cd,
                # filename=input_file_name
                transform_name=self.transform_name)
            return self.upload_id

    def update(self, end_date=True, **args):
        '''Update SQL fields using python arguments.
        For example::

           r.update(load_status='OK')
        '''
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
        with self.engine.begin() as work:
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
        with self.engine.begin() as conn:
            # ISSUE: use sqlalchemy reflection instead?
            exists_q = '''
            select patient_num from {schema}.{table}
            where 1 = 0
            '''.format(schema=self.schema_name, table=self.table_eg)
            try:
                conn.execute(sql_text(exists_q))
                return True
            except DatabaseError:
                return False


class ReportTask(DBAccessTask):
    @property
    def script(self):
        raise NotImplementedError('subclass must implement')

    @property
    def report_name(self):
        raise NotImplementedError('subclass must implement')

    def output(self):
        return CSVTarget(path=self.report_name + '.csv')

    def run(self):
        with self._dbtarget().engine.begin() as conn:
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
