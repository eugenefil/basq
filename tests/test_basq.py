# TODO describe test database
# TODO tests:
# multiple parameterized queries: empty string in one-column csv
# parameterized query: superfluous spaces in header
# parameterized query: unknown input type
# select null
# select memo (binary)
# parameterized query: input null
# parameterized query: input into memo (binary)

import subprocess
import os
import os.path as path
import csv
from io import StringIO
import shutil
from collections import OrderedDict

import pytest


# must be relative, otherwise tmpdb fixture will break
DBPATH = 'vfpdb/db.dbc'


# add path to basq to PATH
os.environ['PATH'] = (
    path.dirname(path.dirname(__file__)) + ':' + os.environ['PATH']
)


class RunError(subprocess.CalledProcessError):
    """Raised when basq returned non-zero exit status."""

    def init(self, returncode, cmd, output=None, stderr=None):
        super().init(returncode, cmd, output=output)
        self.stderr = stderr

    def __str__(self):
        msg = super().__str__()
        return "%s Captured stderr:\n%s" % (msg, self.stderr)


def run(args, input):
    """Run process, pipe input to it and return its captured output.

    Output is returned as a tuple (stdout, stderr). If program exits
    with non-zero code, throw RunError exception.
    """
    # use binary streams (universal_newlines=False) and encode/decode
    # manually, otherwise \r\n in returned query string fields gets
    # converted to \n, i.e. data gets corrupted
    p = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = [
        s.decode('utf-8')
        for s in p.communicate(input.encode('utf-8'))
    ]

    if p.returncode != 0:
        raise RunError(p.returncode, p.args, output=out, stderr=err)

    return out, err


def execsql(
        sql,
        input_rows=None,
        typed_header=False,
        delimiter=None,
        autocommit=False
    ):
    """Exec sql with basq and return rows from output csv if any.

    sql is a query to execute.

    If passed, input_rows must a be a list of rows of input values for
    parameterized query. This list is converted to csv and piped to
    basq right after sql. If row of values is a dict, the parameter
    style is named and no header row is needed. Otherwise it's
    positiotal (question mark) style and first row must be a header.

    sql may be a list of queries. In this case if not None, input_rows
    must be a list of lists of rows one for each parameterized query in
    sql. Parameter styles must be the same for all queries in sql.

    Returned rows are a list of lists including header. If typed_header
    is True, basq must return typed header: each column will contain
    its type delimited from name by space.

    If not None, use delimiter as CSV delimiter.

    If autocommit is True, basq executes in autocommit mode.
    """
    csvargs = {'delimiter': delimiter} if delimiter else {}
    delimiter_arg = ['-t'] if delimiter == '\t' else []

    # if sql is a string, make it and input_rows a one-item list
    if hasattr(sql, 'upper'):
        sql = [sql]
        input_rows = [input_rows]
    # if sql is already a list, but input_rows is None, make
    # input_rows a corresponding list of Nones
    else:
        input_rows = input_rows or [None] * len(sql)

    paramstyle_arg = []
    input_chunks = []
    for query, rows in zip(sql, input_rows):
        input_chunk = query + '\n'
        if rows:
            paramstyle_arg = ['-paramstyle']
            f = StringIO()
            row1 = rows[0]
            try:
                colnames = row1.keys()
            except AttributeError:
                paramstyle_arg += ['qmark']
                writer = csv.writer(f, **csvargs)
            else:
                paramstyle_arg += ['named']
                writer = csv.DictWriter(f, colnames, **csvargs)
                writer.writeheader()
            writer.writerows(rows)
            input_chunk += f.getvalue()
            
        input_chunks.append(input_chunk)

    # add empty line separators between parameterized queries
    sep = '\n' if paramstyle_arg else ''
    input = sep.join(input_chunks)

    cmd = (
        ['basq'] +
        paramstyle_arg +
        (['-typed-header'] if typed_header else []) +
        delimiter_arg +
        (['-autocommit'] if autocommit else []) +
        ['vfp', DBPATH]
    )

    out, err = run(cmd, input)
    return list(csv.reader(StringIO(out), **csvargs))


def selectsql(*column_defs, rowcount=1):
    """Return SELECT query to retrieve specified rows and columns.

    Each column definition in column_defs must be a valid sql
    expression.

    `dummy' table in the test database is queried. It has a single
    integer column `n' with increasing values: 0, 1, ... To get rowcount
    rows from it `where n < rowcount' is used.
    """
    assert rowcount < 3 # 2 rows in the table now
    return "select %s from dummy where n < %d" % (
        ', '.join(column_defs),
        rowcount
    )


def select(
        *column_defs,
        input_rows=None,
        rowcount=1,
        typed_header=False,
        delimiter=None
    ):
    """Return rowcount rows of columns specified with column_defs.

    First returned row is a header. See selectsql() and execsql() for
    further details.
    """
    return execsql(
        selectsql(*column_defs, rowcount=rowcount),
        input_rows=input_rows,
        typed_header=typed_header,
        delimiter=delimiter
    )


def selectvalue(expr):
    """Return value specified by sql expression."""
    # Second dummy column '0' is added, because empty string value in
    # one-column csv is written as empty line which is later parsed as
    # row with no values instead of row with one empty string value.
    return select(expr, '0')[1][0]


@pytest.mark.parametrize(
    'testid,expr,expected_value',
    [
        # strings (note additional quotes in expressions)
        ('sample-string', "'test'", 'test'),
        ('empty-string', "''", ''),
        # trailing spaces are trimmed when char type is wider than value
        ('no-trailing-spaces', "cast('test' as char(100))", 'test'),
        ('non-ascii-string', "'Привет, мир!'", 'Привет, мир!'),
        ('special-chars-in-string', "'1' + chr(13) + chr(10) + '2'", '1\r\n2'),
        ('csv-delimiter-in-string', "'1,2'", '1,2'),
        ('doublequotes-in-string', "'Say \"hi\"'", 'Say "hi"'),
        # note: passing long strings gives driver error
        ('memo', "cast('test' as memo)", 'test'),

        # numeric type
        ('sample-numeric', '123.456', '123.456'),
        # width and precision do not affect output
        ('big-width-and-precision', 'cast(1.2 as numeric(20, 10))', '1.2'),
        ('numeric-is-float', '1', '1.0'),
        ('negative-numeric', '-1', '-1.0'),

        # integers
        # a standalone integer number is numeric, so use cast
        ('sample-integer', 'cast(123 as integer)', '123'),
        ('negative-integer', 'cast(-100 as integer)', '-100'),

        # dates
        ('sample-date', 'date(1999, 12, 31)', '1999-12-31'),

        # booleans
        ('boolean-true', '.t.', '1'),
        ('boolean-false', '.f.', '0')
    ]
)        
def test_select_value(testid, expr, expected_value):
    """Test how values are returned from database.

    expr must be an sql expression which is substituted into SELECT
    query and must return expected_value.

    testid uniquely identifies the test and is used to select specific
    tests to run on py.test command line with -k. Spaces in testid are
    not allowed.
    """
    assert selectvalue(expr) == expected_value


def test_select():
    assert select('n', 'n + 1 as next', rowcount=2) == [
        ['n', 'next'],
        ['0', '1.0'], # n is integer, but n + 1 becomes numeric
        ['1', '2.0']
    ]


def test_select_with_no_rows():
    assert select('n', rowcount=0) == [['n']]


def test_select_with_typed_header():
    assert select(
        "'john' as name",
        'cast(40 as int) as age',
        '73.5 as weight',
        'date(1980, 1, 1) as birth',
        "cast('hacker' as memo) occupation",
        '.t. as cool',
        typed_header=True
    )[0] == [
        'name string',
        'age integer',
        'weight number',
        'birth date',
        'occupation string',
        'cool boolean'
    ]


@pytest.mark.parametrize(
    'value_id,in_value,in_type,out_value,out_type',
    [
        ('string', '\r\n', 'string', None, None),
        ('non-ascii', 'Привет, мир!', 'string', None, None),
        ('number', '1.0', 'number', None, None),
        # Due to the bug in vfp oledb provider (see comments in basq
        # input type parsers), ints cannot be passed to parameterized
        # queries. Instead they are converted to floats on input.
        ('integer', '1', 'integer', '1.0', 'number'),
        ('date', '1999-12-31', 'date', None, None),
        # input value with no type is assumed to be string
        ('no-type-is-string', '1', '', None, 'string'),
        ('boolean-true', '1', 'boolean', None, None),
        ('boolean-false', '0', 'boolean', None, None)
    ]
)
@pytest.mark.parametrize(
    'paramstyle,param,rowfunc',
    [
        (
            'positional',
            '?',
            lambda hdr_row, val_row, : [hdr_row, val_row]
        ),
        (
            'named',
            ':in',
            lambda hdr_row, val_row: [dict(zip(hdr_row, val_row))]
        )
    ]
)
def test_pass_value_to_parameterized_query(
        value_id,
        in_value,
        in_type,
        out_value,
        out_type,
        paramstyle,
        param,
        rowfunc
    ):
    """Test passing each value type with each parameter style.

    The value is passed to SELECT, then output is read. If output value
    and type are the same as input, then value was passed correctly.

    If we know that passing each value type parameterized works fine,
    there is no need to test each value type with each kind of sql
    query. For example, if passing dates parameterized works with
    SELECT, then passing dates works in general and it will work with
    INSERT, UPDATE and DELETE.

    If out_value is None, it is assumed the same as in_value. Same for
    out_type.
    """
    out_value = in_value if out_value is None else out_value
    out_type = in_type if out_type is None else out_type

    assert select(param + ' as out', typed_header=True, input_rows=rowfunc(
        [('in ' + in_type).rstrip()],
        [in_value]
    )) == [
        ['out ' + out_type],
        [out_value]
    ]


def test_pass_long_string_to_parameterized_query():
    """Test passing long strings for memo columns.

    Long strings cannot be passed inline in the query, only via
    parameters.
    """
    longstring = 'a' * 1024
    assert select(
        # cast to memo, 'string is too long' error otherwise
        'cast(? as memo) as out',
        typed_header=True,
        input_rows=[
            ['in string'],
            [longstring]
        ]
    ) == [
        ['out string'],
        [longstring]
    ]


def test_parameterized_select():
    assert select('?', '?', input_rows=[
        ['name string', 'score number'],
        ['john', '5.0']
    ])[1:] == [['john', '5.0']]


def test_parameterized_select_with_no_input_values():
    assert select('?', input_rows=[['name string']]) == []


def test_parameterized_query_with_named_params():
    """Test named parameters mechanism.

    If it works fine with SELECT, it works fine with others.
    """
    # note the order of columns in output is inverted compared to
    # input
    assert select(':name', ':score', input_rows=[OrderedDict((
        ('score number', '5.0'), ('name string', 'john')
    ))])[1:] == [['john', '5.0']]


@pytest.fixture
def tmpdb(tmpdir):
    """Create temp database.

    Copy test database to temp directory and chdir to it. chdir back on
    teardown.

    We have to chdir and keep DBPATH relative, because basq is a
    Windows program, but when testing in Cygwin tmpdir will be a unix
    path, which will break basq if absolute path is given to it.
    """
    origcwd = os.getcwd()

    origdbdir, dbname = path.split(DBPATH)
    tmpdbdir = tmpdir.join(origdbdir)
    shutil.copytree(origdbdir, tmpdbdir)
    os.chdir(tmpdir)
    yield None

    os.chdir(origcwd)


def test_insert(tmpdb):
    # table person is initially empty
    execsql("insert into person values (1, 'john')")
    assert execsql("select * from person")[1:] == [['1', 'john']]


def test_parameterized_insert(tmpdb):
    execsql(
        "insert into person values (?, ?)",
        input_rows=[
            ['id integer', 'name string'],
            ['1', 'john'],
            ['2', 'bill']
        ]
    )
    assert execsql("select * from person")[1:] == [
        ['1', 'john'],
        ['2', 'bill']
    ]


def test_update(tmpdb):
    execsql("update guy set name = 'ed' where id = 2")
    assert execsql("select name from guy where id = 2")[1] == ['ed']


def test_parameterized_update(tmpdb):
    execsql(
        "update guy set name = ? where id = ?",
        input_rows=[
            ['name string', 'id integer'],
            ['johnny', '1'],
            ['billy', '2']
        ]
    )
    assert execsql("select * from guy")[1:] == [
        ['1', 'johnny'],
        ['2', 'billy']
    ]


def test_delete(tmpdb):
    execsql("delete from guy where id = 1")
    assert execsql("select * from guy where id = 1")[1:] == []


def test_parameterized_delete(tmpdb):
    execsql(
        "delete from guy where id = ? and name = ?",
        input_rows=[
            ['id integer', 'name string'],
            ['1', 'john'],
            ['2', 'bill']
        ]
    )
    assert execsql("select * from guy")[1:] == []


def test_many_input_queries(tmpdb):
    execsql([
        "insert into person values (1, 'john')",
        "update person set name = 'bill' where id = 1"
    ])
    assert execsql("select * from person")[1:] == [['1', 'bill']]


@pytest.mark.parametrize(
    'testid,line',
    [
        ('empty', ''),
        ('cr', '\r'),
        ('spaces', ' '),
        ('tabs', '\t')
    ]
)
def test_ignore_whitespace_input_lines(testid, line):
    assert execsql(line + '\n') == []


def test_input_queries_done_in_transaction(tmpdb):
    with pytest.raises(RunError) as excinfo:
        execsql([
            # first query is correct
            "insert into person values (1, 'john')",
            # second query leads to db engine error
            "update"
        ])
    assert excinfo.value.returncode == 1

    assert execsql("select * from person")[1:] == []


def test_many_input_parameterized_queries(tmpdb):
    execsql(
        [
            "insert into person values (?, ?)",
            "update person set name = ? where id = ?"
        ],        
        input_rows=[
            [
                ['id integer', 'name string'],
                ['1', 'john']
            ],
            [
                ['name string', 'id integer'],
                ['bill', '1']
            ]
        ]
    )
    assert execsql("select * from person")[1:] == [['1', 'bill']]


def test_tsv_input_output():
    rows = [
        ['id', 'name'],
        ['1', 'john']
    ]
    assert select(
        '? as id', '? as name',
        input_rows=rows,
        delimiter='\t'
    ) == rows


def test_ddl(tmpdb):
    execsql(
        'create table item (id integer, name char(10))',
        # FoxPro DDL statements work only in autocommit mode
        autocommit=True
    )
    execsql("insert into item values (1, 'box')")
    assert execsql("select * from item")[1:] == [['1', 'box']]
