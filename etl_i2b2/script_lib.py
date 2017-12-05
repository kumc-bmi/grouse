r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.cms_patient_mapping.title
    'view of CMS beneficiaries'

    >>> text = Script.cms_patient_mapping.value
    >>> lines = text.split('\n')
    >>> print(lines[0])
    /** cms_patient_mapping - view of CMS beneficiaries

We can separate the script into statements::

    >>> statements = Script.cms_patient_dimension.statements()
    >>> print(next(s for s in statements if 'insert' in s))
    ... #doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
    using (
    ...

A bit of sqlplus syntax is supported for ignoring errors in just part
of a script:

    >>> Script.sqlerror('whenever sqlerror exit')
    False
    >>> Script.sqlerror('whenever sqlerror continue')
    True
    >>> Script.sqlerror('select 1 + 1 from dual') is None
    True

Dependencies between scripts are declared as follows::

    >>> print(next(decl for decl in statements if "'dep'" in decl))
    ... #doctest: +ELLIPSIS
    select ethnicity_cd from "&&I2B2STAR".patient_dimension where 'dep' = 'pdim_add_cols.sql'

    >>> Script.cms_patient_mapping.deps()
    ... #doctest: +ELLIPSIS
    [<Package(cms_keys)>]

The `.pls` extension indicates a dependency on a package rather than a script::

    >>> _sorted = lambda s: sorted(s, key=lambda x: x.name)
    >>> _sorted(Script.cms_dem_txform.deps())
    [<Package(cms_keys)>, <Script(i2b2_crc_design)>]

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.i2b2_crc_design.created_objects()
    [view no_value, view valtype_cd, view tval_char, view i2b2_status, view i2b2_crc_design]

as well as tables inserted into::

    >>> variables={I2B2STAR: 'I2B2DEMODATA',
    ...            CMS_RIF: 'CMS_DEID', 'upload_id': '20', 'chunk_qty': 20,
    ...            'cms_source_cd': "'ccwdata.org'", 'source_table': 'T'}
    >>> Script.bene_chunks_survey.inserted_tables(variables)
    ['bene_chunks']

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print(statements[-1])
    select 1 complete
    from "&&I2B2STAR".patient_dimension pd
    where pd.upload_id =
      (select max(upload_id)
      from "&&I2B2STAR".upload_status up
      where up.transform_name = :task_id
      and load_status = 'OK'
      )
      and rownum = 1

The completion test may depend on a digest of the script and its dependencies:

    >>> design_digest = Script.cms_dem_txform.digest()
    >>> last = Script.cms_dem_txform.statements(variables)[-1].strip()
    >>> print(last)
    select 1 up_to_date
    from cms_dem_txform where design_digest = 880791033

Some scripts use variables that are not known until a task is run; for
example, `&&upload_id` is used in names of objects such as tables and
partitions; these scripts must not refer to such variables in their
completion query:

    >>> del variables['upload_id']
    >>> print(Script.migrate_fact_upload.statements(variables,
    ...     skip_unbound=True)[-1].strip())
    commit

'''

from itertools import groupby
from typing import Dict, Iterable, List, Optional, Sequence, Text, Tuple, Type
from zlib import adler32
import enum
import re
import abc

import pkg_resources as pkg

import sql_syntax
from sql_syntax import (
    Environment, StatementInContext, ObjectId, SQL, Name,
    iter_statement)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts
CMS_RIF = 'CMS_RIF'

ScriptStep = Tuple[int, Text, SQL]
Filename = str


class SQLMixin(enum.Enum):
    @property
    def sql(self) -> SQL:
        from typing import cast
        return cast(SQL, self.value)  # hmm...

    @property
    def fname(self) -> str:
        return self.name + self.extension

    @abc.abstractproperty
    def extension(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def parse(self, text: SQL) -> Iterable[StatementInContext]:
        raise NotImplementedError

    def each_statement(self,
                       variables: Optional[Environment]=None,
                       skip_unbound: bool=False) -> Iterable[ScriptStep]:
        for line, comment, statement in self.parse(self.sql):
            try:
                ss = sql_syntax.substitute(statement, self._all_vars(variables))
            except KeyError:
                if skip_unbound:
                    continue
                else:
                    raise
            yield line, comment, ss

    def _all_vars(self, variables: Optional[Environment]) -> Optional[Environment]:
        '''Add design_digest to variables.
        '''
        if variables is None:
            return None
        return dict(variables, design_digest=str(self.digest()))

    def statements(self,
                   variables: Optional[Environment]=None,
                   skip_unbound: bool=False) -> Sequence[Text]:
        return list(stmt for _l, _c, stmt
                    in self.each_statement(skip_unbound=skip_unbound,
                                           variables=variables))

    def created_objects(self) -> List[ObjectId]:
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
        return [child
                for sql in self.statements()
                for child in Script._get_deps(sql)]

    def dep_closure(self) -> List['SQLMixin']:
        return [self] + [descendant
                         for sql in self.statements()
                         for child in Script._get_deps(sql)
                         for descendant in child.dep_closure()]

    def digest(self) -> int:
        '''Hash the text of this script and its dependencies.

        Unlike the python hash() function, this digest is consistent across runs.
        '''
        return adler32(str(self._text()).encode('utf-8'))

    def _text(self) -> List[str]:
        '''Get the text of this script and its dependencies.

        >>> nodeps = Script.i2b2_crc_design
        >>> nodeps._text() == [nodeps.value]
        True

        >>> complex = Script.cms_dem_txform
        >>> complex._text() != [complex.value]
        True
        '''
        return sorted(set(s.sql for s in self.dep_closure()))

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
        from typing import cast

        m = re.search(r"select \S+ from \S+ where 'dep' = '([^']+)'", sql)
        if not m:
            return []
        name, ext = m.group(1).rsplit('.', 1)
        choices = Script if ext == 'sql' else Package if ext == 'pls' else []
        deps = [cast(SQLMixin, s) for s in choices if s.name == name]
        if not deps:
            raise KeyError(name)
        return deps

    @classmethod
    def sqlerror(cls, s: SQL) -> Optional[bool]:
        if s.strip().lower() == 'whenever sqlerror exit':
            return False
        elif s.strip().lower() == 'whenever sqlerror continue':
            return True
        return None


class ScriptMixin(SQLMixin):
    @property
    def extension(self) -> str:
        return '.sql'

    def parse(self, text: SQL,
              block_sep: str=';\n/\n') -> Iterable[StatementInContext]:
        return (sql_syntax.iter_blocks(text) if block_sep in text
                else iter_statement(text))

    def created_objects(self) -> List[ObjectId]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in sql_syntax.created_objects(stmt)]

    def inserted_tables(self,
                        variables: Optional[Environment]={}) -> List[Name]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in sql_syntax.inserted_tables(
                        sql_syntax.substitute(stmt, self._all_vars(variables)))]


class Script(ScriptMixin, enum.Enum):
    '''Script is an enum.Enum of contents.

    ISSUE: It's tempting to consider separate libraries for NAACCR,
           NTDS, etc., but that doesn't integrate well with the
           generic luigi.EnumParameter() in etl_tasks.SqlScriptTask.

    '''
    [
        # Keep sorted
        bene_chunks_create,
        bene_chunks_survey,
        cdm_harvest_init,
        cms_dem_dstats,
        cms_dem_txform,
        cms_drug_dstats,
        cms_dx_dstats,
        cms_enc_dstats,
        cms_patient_dimension,
        cms_patient_mapping,
        cms_visit_dimension,
        concept_dimension_fill,
        i2b2_crc_design,
        mapping_reset,
        medpar_encounter_map,
        migrate_fact_upload,
        obs_fact_pipe,
        pdim_add_cols,
        synpuf_txform,
        vdim_add_cols,
    ] = [
        pkg.resource_string(__name__,
                            'sql_scripts/' + fname).decode('utf-8')
        for fname in [
                'bene_chunks_create.sql',
                'bene_chunks_survey.sql',
                'cdm_harvest_init.sql',
                'cms_dem_dstats.sql',
                'cms_dem_txform.sql',
                'cms_drug_dstats.sql',
                'cms_dx_dstats.sql',
                'cms_enc_dstats.sql',
                'cms_patient_dimension.sql',
                'cms_patient_mapping.sql',
                'cms_visit_dimension.sql',
                'concept_dimension_fill.sql',
                'i2b2_crc_design.sql',
                'mapping_reset.sql',
                'medpar_encounter_map.sql',
                'migrate_fact_upload.sql',
                'obs_fact_pipe.sql',
                'pdim_add_cols.sql',
                'synpuf_txform.sql',
                'vdim_add_cols.sql',
        ]
    ]

    def __repr__(self) -> str:
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


class PackageMixin(SQLMixin):
    @property
    def extension(self) -> str:
        return '.pls'

    def parse(self, txt: SQL) -> Iterable[StatementInContext]:
        return sql_syntax.iter_blocks(txt)


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


def _object_to_creators(libs: List[Type[SQLMixin]]) -> Dict[ObjectId, List[SQLMixin]]:
    '''Find creator scripts for each object.

    "There can be only one."
    >>> creators = _object_to_creators([Script, Package])
    >>> [obj for obj, scripts in creators.items()
    ...  if len(scripts) > 1]
    []
    '''
    fst = lambda pair: pair[0]
    snd = lambda pair: pair[1]

    objs = sorted(
        [(obj, s)
         for lib in libs for s in lib
         for obj in s.created_objects()],
        key=fst)
    by_obj = groupby(objs, key=fst)
    return dict((obj, list(map(snd, places))) for obj, places in by_obj)


_redefined_objects = [
    obj for obj, scripts in _object_to_creators([Script, Package]).items()
    if len(scripts) > 1]
assert _redefined_objects == [], _redefined_objects
