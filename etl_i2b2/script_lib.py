r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.cms_patient_mapping.title
    'map CMS beneficiaries to i2b2 patients'

    >>> text = Script.cms_patient_mapping.value
    >>> lines = text.split('\n')
    >>> print(lines[0])
    /** cms_patient_mapping - map CMS beneficiaries to i2b2 patients

TODO: copyright, license blurb enforcement

We can separate the script into statements::

    >>> statements = Script.cms_patient_mapping.statements()
    >>> print(next(s for s in statements if 'insert' in s))
    ... #doctest: +ELLIPSIS
    insert /*+ append */
      into "&&I2B2STAR".patient_mapping
    ...

Dependencies between scripts are declared as follows::

    >>> print(next(decl for decl in statements if "'dep'" in decl))
    select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql'

    >>> Script.cms_patient_mapping.deps()
    [<Script(i2b2_crc_design)>, <Script(cms_dem_txform)>]

The `.pls` extension indicates a dependency on a package rather than a script::

    >>> Script.cms_dem_txform.deps()
    [<Script(i2b2_crc_design)>, <Package(cms_keys)>]

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.i2b2_crc_design.created_objects()
    [(<ObjectType.view: 'view'>, 'i2b2_status')]

as well as tables inserted into::

    >>> variables={I2B2STAR: 'I2B2DEMODATA',
    ...            CMS_RIF: 'CMS_DEID',
    ...            'cms_source_cd': "'ccwdata.org'", 'fact_view': 'F'}
    >>> Script.cms_facts_load.inserted_tables(variables)
    ['"I2B2DEMODATA".observation_fact']

TODO: indexes.
ISSUE: truncate, delete, update aren't reversible.

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print(statements[-1])
    select count(*) loaded_record
    from "&&I2B2STAR".patient_mapping

The completion test may depend on a digest of the script and its dependencies:

    >>> design_digest = Script.cms_dem_txform.digest()
    >>> last = Script.cms_dem_txform.statements(variables)[-1].strip()
    >>> print(last.replace(str(design_digest), '123...'))
    select 1 up_to_date
    from cms_dem_txform where design_digest = 123...

'''

from typing import Iterable, List, Optional, Sequence, Set, Text, Tuple
import enum
import re
import abc

import pkg_resources as pkg

from sql_syntax import (
    Environment, StatementInContext, ObjectType, SQL, Name,
    iter_statement, iter_blocks, substitute, params_of,
    created_objects, inserted_tables)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts
CMS_RIF = 'CMS_RIF'


ScriptStep = Tuple[Optional[int], Text, SQL, Environment]
Filename = str   # issue: are filenames bytes?


class SQLMixin(enum.Enum):
    @property
    def sql(self) -> SQL:
        return self.value

    @abc.abstractmethod
    def parse(self, text: SQL) -> Iterable[StatementInContext]:
        raise NotImplementedError

    def each_statement(self,
                       params: Optional[Environment]=None,
                       variables: Optional[Environment]=None) -> Iterable[ScriptStep]:
        for line, comment, statement in self.parse(self.sql):
            ss = substitute(statement, self._all_vars(variables))
            yield line, comment, ss, params_of(ss, params or {})

    def _all_vars(self, variables: Optional[Environment]) -> Optional[Environment]:
        '''Add design_digest to variables.
        '''
        if variables is None:
            return None
        return dict(variables, design_digest=str(self.digest()))

    def statements(self,
                   variables: Optional[Environment]=None) -> Sequence[Text]:
        return list(stmt for _l, _c, stmt, _p
                    in self.each_statement(variables=variables))

    def created_objects(self) -> List[Tuple[ObjectType, Name]]:
        return []

    def inserted_tables(self,
                        variables: Environment={}) -> List[Name]:
        return []

    @property
    def title(self) -> Text:
        line1 = self.sql.split('\n', 1)[0]
        if not (line1.startswith('/** ') and ' - ' in line1):
            raise ValueError('%s missing title block' % self)
        return line1.split(' - ', 1)[1]

    def deps(self) -> List['SQLMixin']:
        # TODO: takewhile('select' in script)
        return [child
                for sql in self.statements()
                for child in Script._get_deps(sql)]

    def dep_closure(self) -> List['SQLMixin']:
        # TODO: takewhile('select' in script)
        return [self] + [descendant
                         for sql in self.statements()
                         for child in Script._get_deps(sql)
                         for descendant in child.dep_closure()]

    def digest(self) -> int:
        '''Hash the text of this script and its dependencies.

        >>> nodeps = Script.i2b2_crc_design
        >>> nodeps.digest() == hash(frozenset([nodeps.value]))
        True

        >>> complex = Script.cms_dem_txform
        >>> complex.digest() != hash(frozenset([complex.value]))
        True
        '''
        return hash(frozenset(s.sql for s in self.dep_closure()))

    @classmethod
    def _get_deps(cls, sql: Text) -> List['SQLMixin']:
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
        name, ext = m.group(1).rsplit('.', 1)
        choices = Script if ext == 'sql' else Package if ext == 'pls' else []
        deps = [s for s in choices if s.name == name]  # type: ignore # mypy issue #2305
        if not deps:
            raise KeyError(name)
        return deps


class ScriptMixin(SQLMixin):
    def parse(self, text: SQL) -> Iterable[StatementInContext]:
        return iter_statement(text)

    def created_objects(self) -> List[Tuple[ObjectType, Name]]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in created_objects(stmt)]

    def inserted_tables(self,
                        variables: Optional[Environment]={}) -> List[Name]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in inserted_tables(
                        substitute(stmt, self._all_vars(variables)))]


class Script(ScriptMixin, enum.Enum):
    '''Script is an enum.Enum of (fname, contents) tuples.

    ISSUE: It's tempting to consider separate libraries for NAACCR,
           NTDS, etc., but that doesn't integrate well with the
           generic luigi.EnumParameter() in etl_tasks.SqlScriptTask.

    '''
    [
        # Keep sorted
        cms_dem_dstats,
        cms_dem_txform,
        cms_dx_dstats,
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
        pkg.resource_string(__name__,
                            'sql_scripts/' + fname).decode('utf-8')
        for fname in [
                'cms_dem_dstats.sql',
                'cms_dem_txform.sql',
                'cms_dx_dstats.sql',
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

    def __repr__(self) -> str:
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


class PackageMixin(SQLMixin):
    def parse(self, txt: SQL) -> Iterable[StatementInContext]:
        return iter_blocks(txt)


class Package(PackageMixin, enum.Enum):
    [
        # Keep sorted
        cms_keys,
    ] = [
        pkg.resource_string(__name__,
                            'sql_scripts/' + fname).decode('utf-8')
        for fname in [
                'cms_keys.pls',
        ]
    ]

    def __repr__(self) -> str:
        return '<%s(%s)>' % (self.__class__.__name__, self.name)
