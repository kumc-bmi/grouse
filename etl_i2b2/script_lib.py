r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.cms_patient_mapping.title
    u'map CMS beneficiaries to i2b2 patients'

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
    ... #doctest: +ELLIPSIS
    select birth_date from cms_... where 'dep' = 'cms_dem_txform.sql'

    >>> Script.cms_patient_mapping.deps()
    ... #doctest: +ELLIPSIS
    frozenset([<Script(cms_dem_txform)>])

The `.pls` extension indicates a dependency on a package rather than a script::

    >>> Script.cms_dem_txform.deps()
    frozenset([<Script(i2b2_crc_design)>, <Package(cms_keys)>])

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.i2b2_crc_design.created_objects()
    [('view', u'i2b2_status')]

as well as tables inserted into::

    >>> variables={I2B2STAR: 'I2B2DEMODATA',
    ...            CMS_RIF: 'CMS_DEID',
    ...            'cms_source_cd': "'ccwdata.org'", 'fact_view': 'F'}
    >>> Script.cms_facts_load.inserted_tables(variables)
    [u'"I2B2DEMODATA".observation_fact']

To insert in chunks by bene_id, define a view of distinct relevant
bene_ids and use the relevant params in your insert statement:

    >>> ChunkByBene.source_view, ChunkByBene.required_params
    ('bene_id_chunk_source', frozenset(['bene_id_hi', 'bene_id_lo']))

    >>> chunked = Script.cms_encounter_mapping
    >>> from sql_syntax import param_names, created_objects
    >>> [ix for (ix, s) in enumerate(chunked.statements())
    ...  if ('view', ChunkByBene.source_view) in created_objects(s)]
    [3, 7]
    >>> [ix for (ix, s) in enumerate(chunked.statements())
    ... if ChunkByBene.required_params <= set(param_names(s)) ]
    [4, 8]

TODO: indexes.
ISSUE: truncate, delete, update aren't reversible.

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print statements[-1]
    select 1 complete
    from "&&I2B2STAR".patient_mapping
    where rownum = 1

The completion test may depend on a digest of the script and its dependencies:

    >>> design_digest = Script.cms_dem_txform.digest()
    >>> last = Script.cms_dem_txform.statements(variables)[-1].strip()
    >>> print last.replace(str(design_digest), '123...')
    select 1 up_to_date
    from cms_dem_txform where design_digest = 123...

'''

from itertools import groupby
import re

import enum
import pkg_resources as pkg

from sql_syntax import (
    iter_statement, iter_blocks, substitute,
    created_objects, inserted_tables)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts
CMS_RIF = 'CMS_RIF'


class SQLMixin(object):
    def each_statement(self,
                       variables=None):
        _name, text = self.value
        for line, comment, statement in self.parse(text):
            ss = substitute(statement, self._all_vars(variables))
            yield line, comment, ss

    def _all_vars(self, variables):
        '''Add design_digest to variables.
        '''
        if variables is None:
            return None
        return dict(variables, design_digest=self.digest())

    def statements(self,
                   variables=None):
        _name, text = self.value
        return list(stmt for _l, _c, stmt
                    in self.each_statement(variables=variables))

    def created_objects(self):
        return []

    def inserted_tables(self,
                        variables={}):
        return []

    @property
    def title(self):
        _name, text = self.value
        line1 = text.split('\n', 1)[0]
        if not (line1.startswith('/** ') and ' - ' in line1):
            raise ValueError('%s missing title block' % self)
        return line1.split(' - ', 1)[1]

    def deps(self):
        # TODO: takewhile('select' in script)
        return frozenset(child
                         for sql in self.statements()
                         for child in Script._get_deps(sql))

    def dep_closure(self):
        # TODO: takewhile('select' in script)
        return frozenset(
            [self] + [descendant
                      for sql in self.statements()
                      for child in Script._get_deps(sql)
                      for descendant in child.dep_closure()])

    def digest(self):
        '''Hash the text of this script and its dependencies.

        >>> nodeps = Script.i2b2_crc_design
        >>> nodeps.digest() == hash(frozenset([nodeps.value[1]]))
        True

        >>> complex = Script.cms_dem_txform
        >>> complex.digest() != hash(frozenset([complex.value[1]]))
        True
        '''
        return hash(frozenset(text for s in self.dep_closure()
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
        name, ext = m.group(1).rsplit('.', 1)
        choices = Script if ext == 'sql' else Package if ext == 'pls' else []
        deps = [s for s in choices if s.name == name]
        if not deps:
            raise KeyError(name)
        return deps


class ScriptMixin(SQLMixin):
    @classmethod
    def parse(cls, text):
        return iter_statement(text)

    def created_objects(self):
        return [obj
                for (_name, text) in [self.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in created_objects(stmt)]

    def inserted_tables(self,
                        variables={}):
        return [obj
                for (_name, text) in [self.value]
                for _l, _comment, stmt in iter_statement(text)
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
        cms_enc_txform,
        cms_encounter_mapping,
        cms_facts_load,
        cms_patient_dimension,
        cms_patient_mapping,
        cms_visit_dimension,
        i2b2_crc_design,
        synpuf_txform,
    ] = [
        (fname, pkg.resource_string(__name__,
                                    'sql_scripts/' + fname).decode('utf-8'))
        for fname in [
                'cms_dem_dstats.sql',
                'cms_dem_txform.sql',
                'cms_dx_dstats.sql',
                'cms_dx_txform.sql',
                'cms_enc_dstats.sql',
                'cms_enc_txform.sql',
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


class PackageMixin(SQLMixin):
    @classmethod
    def parse(cls, txt):
        return iter_blocks(txt)


class Package(PackageMixin, enum.Enum):
    [
        # Keep sorted
        cms_keys,
    ] = [
        (fname, pkg.resource_string(__name__,
                                    'sql_scripts/' + fname).decode('utf-8'))
        for fname in [
                'cms_keys.pls',
        ]
    ]

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


class ChunkByBene(object):
    source_view = 'bene_id_chunk_source'
    required_params = frozenset(['bene_id_lo', 'bene_id_hi'])

    ntiles_sql = '''
    select chunk_num, count(*) chunk_size
         , min(bene_id) bene_id_lo, max(bene_id) bene_id_hi
    from (
    select bene_id, ntile({ntiles}) over (order by bene_id) as chunk_num
    from {chunk_source_view}
    ) group by chunk_num
    order by bene_id_lo
    '''

    @classmethod
    def chunk_query(cls, ntiles,
                    chunk_source=None):
        return cls.ntiles_sql.format(
            ntiles=ntiles,
            chunk_source_view=chunk_source or cls.source_view)

    @classmethod
    def result_chunks(cls, result, limit):
        chunks = [dict(bene_id_lo=row.bene_id_lo,
                       bene_id_hi=row.bene_id_hi)
                  for row in result[:limit]]
        sizes = [row.chunk_size for row in result[:limit]]
        return chunks, sizes


def _object_to_creators(libs):
    '''Find creator scripts for each object.

    "There can be only one."
    >>> creators = _object_to_creators([Script, Package])
    >>> [obj for obj, scripts in creators
    ...  if len(scripts) > 1]
    [('view', u'bene_id_chunk_source')]
    '''
    item0 = lambda o_s: o_s[0]
    objs = sorted(
        [(obj, s)
         for lib in libs for s in lib
         for obj in s.created_objects()],
        key=item0)
    by_obj = groupby(objs, key=item0)
    return [(obj, list(places)) for obj, places in by_obj]


_redefined_objects = [
    obj for obj, scripts in _object_to_creators([Script, Package])
    if len(scripts) > 1 and
    obj not in [('view', ChunkByBene.source_view)]]
assert _redefined_objects == [], _redefined_objects
