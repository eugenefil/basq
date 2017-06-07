## output:
# parameterized select
# empty date
# null
# binary

## input:
# insert
# update
# delete
# non-ascii input
# string
# numeric
# date
# empty date
# null
# binary
# parameterized insert
# parameterized update
# parameterized delete

# multiple queries
# transaction rollback on error

# multiple parameterized queries
# multiple parameterized queries (empty strings in one-column data)

import subprocess
import os
import os.path as path
import csv
from io import StringIO

import pytest


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


def execsql(sql, typed_header=False):
    """Exec sql with adosql and return rows parsed from output tsv.

    Returned rows are a list of lists including header. If typed_header
    is True, adosql must return typed header: each column will contain
    its type delimited from name by space.
    """
    cmd = (
        ['adosql'] +
        (['-typed-header'] if typed_header else []) +
        ['vfp', 'vfpdb/db.dbc']
    )
    out, err = run(cmd, sql)
    return list(csv.reader(StringIO(out), delimiter='\t'))


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


def select(*column_defs, rowcount=1, typed_header=False):
    """Return rowcount rows of columns specified with column_defs.

    First row is header. See selectsql() and execsql() for further
    details.
    """
    return execsql(
        selectsql(*column_defs, rowcount=rowcount),
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
        ('tabs-in-string', "'1\t2'", '1\t2'),
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
def test_retrieve_value(testid, expr, expected_value):
    """Test how values are returned from database.

    expr must be an sql expression which is substituted into SELECT
    query and must return expected_value.

    testid uniquely identifies the test and is used to select specific
    tests to run on py.test command line with -k. Spaces in testid are
    not allowed.
    """
    assert selectvalue(expr) == expected_value


def test_retrieve_no_rows():
    "Test case when no rows were returned, only header."
    assert select('n', rowcount=0) == [['n']]


def test_retrieve_many_rows_many_cols():
    assert select('n', 'n + 1 as next', rowcount=2) == [
        ['n', 'next'],
        ['0', '1.0'], # n is integer, but n + 1 becomes numeric
        ['1', '2.0']
    ]


def test_typed_header():
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
