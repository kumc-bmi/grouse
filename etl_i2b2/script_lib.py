r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.cms_patient_mapping.title
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
    frozenset([<Script(i2b2_crc_design)>, <Script(cms_patient_mapping)>, ...

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.i2b2_crc_design.created_objects()
    [(<Script(i2b2_crc_design)>, ('view', 'i2b2_status'))]

as well as tables inserted into::

    >>> variables={'I2B2STAR': 'i2b2demodata',
    ...            'cms_source_cd': Source.cms.value}
    >>> Script.cms_patient_mapping.inserted_tables(variables)
    [(<Script(cms_patient_mapping)>, '"i2b2demodata".patient_mapping')]

TODO: indexes.
ISSUE: truncate, delete, update aren't reversible.

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print statements[-1]
    select count(*) loaded_record
    from "&&I2B2STAR".patient_mapping

The completion test may depend on a digest of the script and its dependencies:

    >>> design_digest = Script.cms_dem_txform.digest()
    >>> last = Script.cms_dem_txform.statements(variables)[-1].strip()
    >>> print last.replace(str(design_digest), '123...')
    TODO: move design_digest to its own view in most cases
    select 1 complete
    from cms_patient_dimension pd, cms_visit_dimension vd
    where pd.design_digest =
      123...
      and vd.design_digest =
      123...
      and rownum <= 1

Sources are also design-time constants:

    >>> Source.cms.value
    'ccwdata.org'

'''

import re

import enum
import pkg_resources as pkg

from sql_syntax import (
    iter_statement, substitute, params_of,
    created_objects, inserted_tables)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts


class ScriptMixin(object):
    def each_statement(self,
                       params=None,
                       variables=None):
        _name, text = self.value
        for line, comment, statement in iter_statement(text):
            ss = substitute(statement, self._all_vars(variables))
            yield line, comment, ss, params_of(ss, params or {})

    def _all_vars(self, variables):
        '''Add design_digest to variables.
        '''
        if variables is None:
            return None
        return dict(variables, design_digest=self.digest())

    def statements(self,
                   variables=None):
        _name, text = self.value
        return list(stmt for _l, _c, stmt, _p
                    in self.each_statement(variables=variables))

    def created_objects(self):
        return [(dep, obj)
                for dep in self.deps()
                for (_name, text) in [dep.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in created_objects(stmt)]

    def inserted_tables(self,
                        variables={}):
        return [(dep, obj)
                for dep in self.deps()
                for (_name, text) in [dep.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in inserted_tables(
                        substitute(stmt, self._all_vars(variables)))]

    @property
    def title(self):
        _name, text = self.value
        line1 = text.split('\n', 1)[0]
        if not (line1.startswith('/** ') and ' - ' in line1):
            raise ValueError('%s missing title block' % self)
        return line1.split(' - ', 1)[1]

    def deps(self):
        # TODO: takewhile('select' in script)
        return frozenset(
            [self] + [descendant
                      for sql in self.statements()
                      for child in Script._get_deps(sql)
                      for descendant in child.deps()])

    def digest(self):
        '''Hash the text of this script and its dependencies.

        >>> nodeps = Script.i2b2_crc_design
        >>> nodeps.digest() == hash(frozenset([nodeps.value[1]]))
        True

        >>> complex = Script.cms_dem_txform
        >>> complex.digest() != hash(frozenset([complex.value[1]]))
        True
        '''
        return hash(frozenset(text for s in self.deps()
                              for _fn, text in [s.value]))

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
        cms_dem_dstats,
        cms_dem_load,
        cms_dem_txform,
        cms_dx_txform,
        cms_enc_dstats,
        cms_encounter_mapping,
        cms_facts_load,
        cms_patient_dimension,
        cms_patient_mapping,
        cms_visit_dimension,
        i2b2_crc_design,
        synpuf_txform,
    ] = [
        (fname, pkg.resource_string(__name__, 'sql_scripts/' + fname))
        for fname in [
                'cms_dem_dstats.sql',
                'cms_dem_load.sql',
                'cms_dem_txform.sql',
                'cms_dx_txform.sql',
                'cms_enc_dstats.sql',
                'cms_encounter_mapping.sql',
                'cms_facts_load.sql',
                'cms_patient_dimension.sql',
                'cms_patient_mapping.sql',
                'cms_visit_dimension.sql',
                'i2b2_crc_design.sql',
                'synpuf_txform.sql',
        ]
    ]

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


class Source(enum.Enum):
    cms = 'ccwdata.org'
