## output:
# parameterized query: superfluous spaces in header
# parameterized query: unknown input type
# null
# binary

## input:
# update
# delete
# parameterized update
# parameterized delete
# null
# binary

# multiple queries on stdin
# transaction rollback on error

# multiple parameterized queries on stdin
# multiple parameterized queries (empty strings in one-column data)

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

CSVSEP = '\t'


# add path to adosql to PATH
os.environ['PATH'] = (
    path.dirname(path.dirname(__file__)) + ':' + os.environ['PATH']
)


class RunError(subprocess.CalledProcessError):
    """Raised when adosql returned non-zero exit status."""

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


def execsql(sql, input_rows=None, typed_header=False):
    """Exec sql with adosql and return rows from output tsv if any.

    If passed, input_rows must a be a list of rows of input values for
    parameterized query. This list is converted to tsv and piped to
    adosql right after sql. If row of values is a dict, the query is a
    named parameterized query and no header row is needed. Otherwise
    it's a positiotal (question mark) query and first row must be a
    header.

    Returned rows are a list of lists including header. If typed_header
    is True, adosql must return typed header: each column will contain
    its type delimited from name by space.
    """
    csvargs = {'delimiter': CSVSEP}

    paramstyle_arg = []
    input = sql
    if input_rows:
        paramstyle_arg = ['-paramstyle']
        f = StringIO()
        row1 = input_rows[0]
        try:
            colnames = row1.keys()
        except AttributeError:
            paramstyle_arg += ['qmark']
            writer = csv.writer(f, **csvargs)
        else:
            paramstyle_arg += ['named']
            writer = csv.DictWriter(f, fieldnames=colnames, **csvargs)
            writer.writeheader()
        writer.writerows(input_rows)
        input += '\n' + f.getvalue()

    cmd = (
        ['adosql'] +
        paramstyle_arg +
        (['-typed-header'] if typed_header else []) +
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
        typed_header=False
    ):
    """Return rowcount rows of columns specified with column_defs.

    First returned row is a header. See selectsql() and execsql() for
    further details.
    """
    return execsql(
        selectsql(*column_defs, rowcount=rowcount),
        input_rows=input_rows,
        typed_header=typed_header
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
        ('csv-sep-in-string', "'1%s2'" % CSVSEP, '1%s2' % CSVSEP),
        ('doublequotes-in-string', "'Say \"hi\"'", 'Say "hi"'),

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
        ('sample-date', 'date(1999, 12, 31)', '1999-12-31')
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
        typed_header=True
    )[0] == [
        'name string',
        'age integer',
        'weight number',
        'birth date'
    ]


data_to_test_parametrized_queries = (
    'testid,in_value,in_type,out_value,out_type',
    [
        ('string', '\r\n', 'string', None, None),
        ('non-ascii', 'Привет, мир!', 'string', None, None),
        ('number', '1.0', 'number', None, None),
        # Due to the bug in vfp oledb provider (see comments in adosql
        # input type parsers), ints cannot be passed to parameterized
        # queries. Instead they are converted to floats on input.
        ('integer', '1', 'integer', '1.0', 'number'),
        ('date', '1999-12-31', 'date', None, None)
    ]
)

@pytest.mark.parametrize(*data_to_test_parametrized_queries)
def test_pass_value_to_parameterized_query(
        testid,
        in_value,
        in_type,
        out_value,
        out_type
    ):
    """Test passing each type of value to parameterized query.

    If out_value is None, it is assumed the same as in_value. Same for
    out_type.

    If we know that passing each type parameterized to ADO works fine,
    there is no need to test each value type with each type of sql
    query. For example, if passing dates parameterized works with
    SELECT, then passing dates works and it will work with INSERT,
    UPDATE, DELETE.
    """
    out_value = in_value if out_value is None else out_value
    out_type = in_type if out_type is None else out_type

    assert select('? as out', typed_header=True, input_rows=[
        ['in ' + in_type],
        [in_value]
    ]) == [
        ['out ' + out_type],
        [out_value]
    ]


def test_parameterized_select():
    assert select('?', '?', input_rows=[
        ['name string', 'score number'],
        ['john', '5.0']
    ])[1:] == [['john', '5.0']]


def test_parameterized_select_with_no_input_values():
    assert select('?', input_rows=[['name string']]) == []


@pytest.mark.parametrize(*data_to_test_parametrized_queries)
def test_pass_value_to_named_parameterized_query(
        testid,
        in_value,
        in_type,
        out_value,
        out_type
    ):
    """Test passing each type of value to named parameterized query.

    Logic is the same as in test for positional parameterized query.
    """
    out_value = in_value if out_value is None else out_value
    out_type = in_type if out_type is None else out_type

    assert select(':in as out', typed_header=True, input_rows=[
        {'in ' + in_type: in_value}
    ]) == [
        ['out ' + out_type],
        [out_value]
    ]


def test_named_parameterized_query():
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

    We have to chdir and keep DBPATH relative, because adosql is a
    Windows program, but when testing in Cygwin tmpdir will be a unix
    path, which will break adosql if absolute path is given to it.
    """
    origcwd = os.getcwd()

    origdbdir, dbname = path.split(DBPATH)
    tmpdbdir = tmpdir.join(origdbdir)
    shutil.copytree(origdbdir, tmpdbdir)
    os.chdir(tmpdir)
    yield None

    os.chdir(origcwd)


def test_insert(tmpdb):
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
