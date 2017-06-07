## output:
# empty date
# null
# binary
# empty output
# typed tsv output
# parameterized select
#
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
#
# multiple queries
# transaction rollback on error
#
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


def run(input):
    """Run adosql, pipe input to it and return its captured output.

    Output is returned as a tuple (stdout, stderr). If program exits
    with non-zero code, throw subprocess.CalledProcessError exception.
    """
    # use binary streams (universal_newlines=False) and encode/decode
    # manually, otherwise \r\n in returned query string fields gets
    # converted to \n, i.e. data gets corrupted
    p = subprocess.Popen(
        ['adosql', 'vfp', 'vfpdb/db.dbc'],
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


def execsql(cmd):
    """Exec cmd with adosql and return rows parsed from output tsv.

    Returned rows are a list of lists including header.
    """
    out, err = run(cmd)
    return list(csv.reader(StringIO(out), delimiter='\t'))


def selectsql(expr):
    """Return sql query to retrieve the expr value.

    expr must a valid sql expression.
    """
    # Query is built as follows. FoxPro requires FROM clause, so just
    # appending expr to SELECT does not work. Thus there is a `dummy'
    # table with a single column `n' with increasing values: 0, 1,
    # ... To get N values from it specify `where n < N'. Second column
    # is added, because empty string in one-column csv is parsed as
    # empty row instead of row with empty string.
    return "select %s, 0 from dummy where n < 1" % expr


def select(expr):
    """Return value specified by sql expression."""
    return execsql(selectsql(expr))[1][0]


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
    assert select(expr) == expected_value


#     assert out == '''
# string	integer	number	date	binary
# line1	1234	12.34	1999-12-31	aGVsbG8=
# line2	5678	0.5678	2000-01-01	d29ybGQ=
# '''.lstrip()
