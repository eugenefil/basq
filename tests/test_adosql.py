## tests:
# select with tsv output (with date, binary, null fields)
# empty output
# utf-8 input/output
# empty dates
# select with typed tsv output
# parameterized select
#
# insert
# update
# delete
# parameterized insert
# parameterized update
# parameterized delete
#
# multiple queries
# transaction rollback on error
# multiple parameterized queries

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


def run(input):
    """Run adosql, pipe input to it and return its captured output.

    Output is returned as a tuple (stdout, stderr). If program exits
    with non-zero code, throw subprocess.CalledProcessError exception.
    """
    p = subprocess.Popen(
        ['adosql', 'vfp', 'vfpdb/db.dbc'],
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = p.communicate(input)

    if p.returncode != 0:
        raise subprocess.CalledProcessError()

    return out, err


def sql(cmd):
    """Run cmd with adosql and return rows parsed from output tsv.

    Returned rows are a list of lists including header.
    """
    out, err = run(cmd)
    return list(csv.reader(StringIO(out), delimiter='\t'))


def select(expr):
    """Return value specified by sql expression.

    The function itself constructs the query to adosql to retrieve the
    value.
    """
    # Query is built as follows. FoxPro requires FROM clause, so just
    # appending expr to SELECT does not work. Thus there is a `dummy'
    # table with a single column `n' with increasing values: 0, 1,
    # ... To get N values from it specify `where n < N'.
    return sql("select %s from dummy where n < 1" % expr)[1][0]


def test_select_string():
    # note the absence of trailing spaces in returned value despite
    # char type being wider than the value
    assert select("cast('string1' as char(10))") == 'string1'


def test_select_numeric():
    assert select("cast(123.456 as numeric(10, 4))") == '123.456'


#     assert out == '''
# string	integer	number	date	binary
# line1	1234	12.34	1999-12-31	aGVsbG8=
# line2	5678	0.5678	2000-01-01	d29ybGQ=
# '''.lstrip()
