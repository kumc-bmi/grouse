import re


def iter_statement(txt):
    r'''Iterate over SQL statements in a script.

    >>> list(iter_statement("drop table foo; create table foo"))
    [(1, '', 'drop table foo'), (1, '', 'create table foo')]

    >>> list(iter_statement("-- blah blah\ndrop table foo"))
    [(2, '-- blah blah\n', 'drop table foo')]

    >>> list(iter_statement("drop /* blah blah */ table foo"))
    [(1, '', 'drop  table foo')]

    .. todo:: handle ';' inside string literals more completely

    >>> list(iter_statement("select '[^;]+' from dual"))
    [(1, '', "select '[^;]+' from dual")]

    >>> list(iter_statement('select "x--y" from z;'))
    [(1, '', 'select "x--y" from z')]
    >>> list(iter_statement("select 'x--y' from z;"))
    [(1, '', "select 'x--y' from z")]
    '''

    statement = comment = ''
    line = 1
    sline = None

    def save(txt):
        return (statement + txt, sline or (line if txt else None))

    while 1:
        m = SQL_SEPARATORS.search(txt)
        if not m:
            statement, sline = save(txt)
            break

        pfx, match, txt = (txt[:m.start()],
                           txt[m.start():m.end()],
                           txt[m.end():])
        if pfx:
            statement, sline = save(pfx)

        if m.group('sep'):
            yield sline, comment, statement
            statement = comment = ''
            sline = None
        elif [n for n in ('lit', 'hint', 'sym')
              if m.group(n)]:
            statement, sline = save(match)
        elif (m.group('space') and statement):
            statement, sline = save(match)
        elif ((m.group('comment') and not statement) or
              (m.group('space') and comment)):
            comment += match

        line += (pfx + match).count("\n")

    if comment or statement:
        yield sline, comment, statement

# Check for hint before comment since a hint looks like a comment
SQL_SEPARATORS = re.compile(
    r'(?P<space>^\s+)'
    r'|(?P<hint>/\*\+.*?\*/)'
    r'|(?P<comment>(--[^\n]*(?:\n|$))|(?:/\*([^\*]|(\*(?!/)))*\*/))'
    r'|(?P<sym>"[^\"]*")'
    r"|(?P<lit>'[^\']*')"
    r'|(?P<sep>;)')


def _test_iter_statement():
    r'''
    >>> list(iter_statement("/* blah blah */ drop table foo"))
    [(1, '/* blah blah */ ', 'drop table foo')]

    >>> [l for (l, c, s) in iter_statement('s1;\n/**********************\n'
    ...                     '* Medication concepts \n'
    ...                     '**********************/\ns2')]
    [1, 5]

    >>> list(iter_statement(""))
    []

    >>> list(iter_statement("/*...*/   "))
    [(None, '/*...*/   ', '')]

    >>> list(iter_statement("drop table foo;   "))
    [(1, '', 'drop table foo')]

    >>> list(iter_statement("drop table foo"))
    [(1, '', 'drop table foo')]

    >>> list(iter_statement("/* *** -- *** */ drop table foo"))
    [(1, '/* *** -- *** */ ', 'drop table foo')]

    >>> list(iter_statement("/* *** -- *** */ drop table x  /* ... */"))
    [(1, '/* *** -- *** */ ', 'drop table x  ')]

    >>> list(iter_statement('select /*+ index(ix1 ix2) */ * from some_table'))
    [(1, '', 'select /*+ index(ix1 ix2) */ * from some_table')]

    >>> len(list(iter_statement(""" /* + no space before + in hints*/
    ...  /*+ index(observation_fact fact_cnpt_pat_enct_idx) */
    ...  select * from some_table;  """))[0][2])
    79

    >>> list(iter_statement('select 1+1; /* nothing else */; '))
    [(1, '', 'select 1+1'), (None, '/* nothing else */', '')]
    '''
    pass  # pragma: nocover


def substitute(sql, variables):
    '''Evaluate substitution variables in the style of Oracle sqlplus.
    '''
    if variables is None:
        return sql
    sql_esc = sql.replace('%', '%%')  # escape %, which we use specially
    return re.sub('&&(\w+)', r'%(\1)s', sql_esc) % variables


def params_of(s, p):
    '''
    >>> params_of('select 1+1 from dual', {'x':1, 'y':2})
    {}
    >>> params_of('select 1+:y from dual', {'x':1, 'y':2})
    {'y': 2}
    '''
    return dict([(k, v) for k, v in p.iteritems()
                 if ':' + k in s])


def created_objects(statement):
    r'''
    >>> created_objects('create table t as ...')
    [('table', 't')]
    >>> created_objects('create or replace view x\nas ...')
    [('view', 'x')]
    '''
    m = re.search('^create or replace view (\S+)', statement.strip())
    views = [('view', m.group(1))] if m else []
    m = re.search('^create table (\S+)', statement.strip())
    tables = [('table', m.group(1))] if m else []
    return tables + views


def inserted_tables(statement):
    r'''
    >>> inserted_tables('create table t as ...')
    []
    >>> inserted_tables('insert into t (...) ...')
    ['t']
    '''
    if not statement.startswith('insert'):
        return []
    m = re.search('into (\S+)', statement.strip())
    return [m.group(1)] if m else []
