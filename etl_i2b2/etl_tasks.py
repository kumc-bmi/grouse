from sqlalchemy import text as sql_text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import DatabaseError
import luigi

from script_lib import Script

CMS_CCW = 'ccwdata.org'  # TODO: sync with cms_ccw_spec.sql
I2B2STAR = 'I2B2STAR'  # TODO: sync with .sql scripts?


class AccountParameter(luigi.parameter.Parameter):
    def parse(self, x):
        return make_url(x)

    def serialize(self, x):
        return str(x)

    def normalize(self, x):
        return self.parse(x)


class SqlScriptTask(luigi.Task):
    script = luigi.EnumParameter(enum=Script)
    account = AccountParameter()
    variables = luigi.DictParameter(default={})
    echo = luigi.BoolParameter(default=True)  # TODO: proper logging

    def task_id_str(self):
        name, _text = self.script.value
        url = self.account
        return '%s:%s@//%s:%s/%s' % (
            name, url.username, url.host, url.port, url.database)

    def requires(self):
        return [SqlScriptTask(script=s,
                              account=self.account,
                              variables=self.variables,
                              echo=self.echo)
                for s in self.script.deps()]

    def __engine(self):
        from sqlalchemy import create_engine  # hmm... ambient...
        # TODO: keep engine around?
        return create_engine(self.account)

    def complete(self):
        '''Each script's last query tells whether it is complete.

        It should be a scalar query that returns non-zero for done
        and either zero or an error for not done.
        '''
        last_query = self.script.statements(
            variables=self.variables)[-1]
        db = self.__engine()
        try:
            return not not db.scalar(sql_text(last_query))
        except DatabaseError:
            return False

    def run(self,
            bind_params={}):
        # TODO: log script_name?
        with self.__engine().begin() as work:
            last_result = None
            for statement in self.script.statements(
                    variables=self.variables):
                # TODO: log and time each statement? row count?
                # structured_logging?
                # launch/build a sub-task for each statement?
                self.set_status_message(statement)
                last_result = work.execute(
                    statement,
                    Script.params_of(statement, bind_params))
            return last_result and last_result.fetchone()


class UploadTask(SqlScriptTask):
    source_cd = luigi.Parameter()  # ISSUE: design-time enum of sources?

    def run(self):
        upload = self.output()
        upload_id = upload.insert(label=self.script.title(),
                                  user_id=self.account.username,
                                  source_cd=self.source_cd)
        last_result = SqlScriptTask.run(
            self, bind_params=dict(upload_id=upload_id))
        upload.update(load_status='OK', loaded_record=last_result[0])

    def output(self):
        star_schema = self.variables[I2B2STAR]
        return UploadTarget(self.account, star_schema,
                            self.script.name)


class UploadTarget(luigi.target.Target):
    def __init__(self, account, star_schema, transform_name,
                 echo=False):
        self.account = account
        self.echo = echo
        self.star_schema = star_schema
        self.transform_name = transform_name

    def __engine(self):
        from sqlalchemy import create_engine  # hmm... ambient...
        # TODO: keep engine around?
        return create_engine(self.account)

    def exists(self):
        with self.__engine().begin() as conn:
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
        with self.__engine().begin() as work:
            self.upload_id = work.scalar(
                sql_text(
                    '''select {i2b2}.sq_uploadstatus_uploadid.nextval
                    from dual'''.format(i2b2=self.star_schema)))

            work.execute(
                """
                insert into nightherondata.upload_status
                (upload_id, upload_label, user_id,
                  source_cd,
                  load_date, transform_name)
                values (:upload_id, :label, :user_id,
                        :source_cd,
                        sysdate, :transform_name)""",
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
        stmt = ('update ' + self.star_schema + '.upload_status ' +
                self._update_set(**args) +
                (', end_date = sysdate' if end_date else '') +
                ' where upload_id = :upload_id')
        with self.__engine().begin() as work:
            work.execute(sql_text(stmt),
                         **dict(args, upload_id=self.upload_id))

    @classmethod
    def _update_set(cls, **args):
        '''
        >>> UploadTarget._update_set(message='done', no_of_record=1234)
        'set no_of_record=:no_of_record, message=:message'
        '''
        return 'set ' + ', '.join(['%s=:%s' % (k, k) for k in args.keys()])
