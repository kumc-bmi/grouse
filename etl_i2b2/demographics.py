'''
Usage:

  (grouse-etl)$ python demographics.py

or:

  (grouse-etl)$ PYTHONPATH=. luigi --module demographics Demographics \
                                   --local-scheduler

TODO: why won't luigi find modules in the current directory?

'''

import re

from luigi.contrib import sqla
import enum
import luigi
import pkg_resources as pkg
from sqlalchemy.engine.url import make_url


class Script(enum.Enum):
    [
        cms_ccw_spec,
        cms_dem_dstats,
        cms_dem_load,
        cms_dem_txform,
        cms_patient_mapping,
        grouse_project,
        i2b2_crc_design
    ] = [
        (fname, pkg.resource_string(__name__, 'sql_scripts/' + fname))
        for fname in [
                'cms_ccw_spec.sql',
                'cms_dem_dstats.sql',
                'cms_dem_load.sql',
                'cms_dem_txform.sql',
                'cms_patient_mapping.sql',
                'grouse_project.sql',
                'i2b2_crc_design.sql'
        ]
    ]

    def statements(self,
                   separator=';\n'):
        _name, text = self.value
        return [
            part.strip()
            for part in text.split(separator)
            if part.strip()]

    @classmethod
    def _get_deps(cls, sql):
        '''
        >>> ds = Script._get_deps(
        ...     "select col from t where 'dep' = 'grouse_project.sql'")
        >>> ds == [Script.grouse_project]
        True

        >>> ds = Script._get_deps(
        ...     "select col from t where 'dep' = 'oops.sql'")
        Traceback (most recent call last):
            ...
        KeyError: 'oops'

        >>> Script._get_deps(
        ...     "select col from t where x = 'name.sql'")
        []
        '''
        m = re.search(r"select \S+ from \S+ where 'dep' = '([^']+)'", sql)
        if not m:
            return []
        name = m.group(1).replace('.sql', '')
        deps = [s for s in Script if s.name == name]
        if not deps:
            raise KeyError(name)
        return deps

    def deps(self):
        return [script
                for sql in self.statements()
                for script in Script._get_deps(sql)]


class SqlScriptTask(luigi.Task):
    script = luigi.EnumParameter(enum=Script)
    account = luigi.Parameter(default='sqlite:///')
    echo = luigi.BoolParameter(default=True)  # TODO: proper logging

    def task_id_str(self):
        name, _text = self.script.value
        account_url = make_url(self.account)
        return '%s:%s@%s' % (name, account_url.username, account_url.database)

    def requires(self):
        return [SqlScriptTask(script=s, account=self.account, echo=self.echo)
                for s in self.script.deps()]

    def connection(self):
        from sqlalchemy import create_engine  # hmm... ambient...
        # TODO: keep engine around?
        return create_engine(self.account).connect()

    def run(self):
        # TODO: log script_name?
        work = self.connection()
        with work.begin():
            for statement in self.script.statements():
                # TODO: log and time each statement? row count?
                # structured_logging?
                print("@@@", statement)
                work.execute(statement)

        with self.output().open('w') as out:
            out.write('@@TADA!')

    def output(self):
        return luigi.LocalTarget(',out/%s' % self.task_id_str())


class Demographics(luigi.Task):
    cdw_account = luigi.Parameter(default='sqlite:///')
    i2b2star_schema = luigi.Parameter(default='NIGHTHERONDATA')
    echo = luigi.BoolParameter(default=True)  # TODO: proper logging

    def requires(self):
        return [SqlScriptTask(Script.cms_patient_mapping,
                              account=self.cdw_account)]

    def run(self):
        pass

    def output(self):
        return sqla.SQLAlchemyTarget(
            connection_string=self.cdw_account,
            target_table='%s.upload_status' % self.i2b2star_schema,
            update_id=self.__class__.__name__,
            echo=self.echo)


if __name__ == '__main__':
    luigi.build([Demographics()], local_scheduler=True)
