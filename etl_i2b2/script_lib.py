r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.cms_patient_mapping.title()
    'map CMS beneficiaries to i2b2 patients'

    >>> fname, text = Script.cms_patient_mapping.value
    >>> lines = text.split('\n')
    >>> print lines[0]
    /** cms_patient_mapping - map CMS beneficiaries to i2b2 patients

TODO: copyright, license blurb enforcement

We can separate the script into statements::

    >>> statements = Script.cms_patient_mapping.statements()
    >>> print next(s for s in statements if 'insert' in s)
    ... #doctest: +ELLIPSIS
    insert /*+ append */
      into "&&I2B2STAR".patient_mapping
    ...

Dependencies between scripts are declared as follows::

    >>> print next(decl for decl in statements if "'dep'" in decl)
    select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql'

    >>> Script.cms_patient_mapping.deps()
    ... #doctest: +ELLIPSIS
    [<Script(i2b2_crc_design)>, <Script(cms_dem_txform)>, ...]

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.cms_ccw_spec.created_objects()
    [(<Script(cms_ccw_spec)>, ('view', 'cms_ccw'))]

as well as tables inserted into::

    >>> Script.cms_patient_mapping.inserted_tables(
    ...     variables={'I2B2STAR': 'i2b2demodata'})
    [(<Script(cms_patient_mapping)>, '"i2b2demodata".patient_mapping')]

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print statements[-1]
    select count(*) complete
    from "&&I2B2STAR".patient_mapping

TODO: indexes.
ISSUE: truncate, delete, update aren't reversible.

'''

import re

import enum
import pkg_resources as pkg

from sql_syntax import (
    iter_statement, substitute,
    created_objects, inserted_tables)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts


class ScriptMixin(object):
    def statements(self,
                   variables=None):
        _name, text = self.value
        return [
            substitute(statement, variables)
            for _line, _comment, statement in iter_statement(text)]

    def created_objects(self):
        return [(dep, obj)
                for dep in ([self] + self.deps())
                for (_name, text) in [dep.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in created_objects(stmt)]

    def inserted_tables(self,
                        variables={}):
        return [(dep, obj)
                for dep in ([self] + self.deps())
                for (_name, text) in [dep.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in inserted_tables(substitute(stmt, variables))]

    def title(self):
        _name, text = self.value
        title = text.split('\n', 1)[0]
        if ' - ' in title:
            title = title.split(' - ', 1)[1]
        return title

    def deps(self):
        # TODO: takewhile('select' in script)
        return [script
                for sql in self.statements()
                for script in Script._get_deps(sql)]

    @classmethod
    def _get_deps(cls, sql):
        '''
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


class Script(ScriptMixin, enum.Enum):
    '''Script is an enum.Enum of (fname, contents) tuples.

    ISSUE: It's tempting to consider separate libraries for NAACCR,
           NTDS, etc., but that doesn't integrate well with the
           generic luigi.EnumParameter() in etl_tasks.SqlScriptTask.

    '''
    [
        # Keep sorted
        cms_ccw_spec,
        cms_dem_dstats,
        cms_dem_load,
        cms_dem_txform,
        cms_patient_dimension,
        cms_patient_mapping,
        i2b2_crc_design
    ] = [
        (fname, pkg.resource_string(__name__, 'sql_scripts/' + fname))
        for fname in [
                'cms_ccw_spec.sql',
                'cms_dem_dstats.sql',
                'cms_dem_load.sql',
                'cms_dem_txform.sql',
                'cms_patient_dimension.sql',
                'cms_patient_mapping.sql',
                'i2b2_crc_design.sql'
        ]
    ]

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self.name)
