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


def test_select_string():
    assert 'string1' == sql(
        "select cast('string1' as char(10)) from dummy where n < 1"
    )[1][0]


def test_select_numeric():
    assert '1.0' == sql(
        "select cast(1 as numeric(10, 4)) from dummy where n < 1"
    )[1][0]


#     assert out == '''
# string	integer	number	date	binary
# line1	1234	12.34	1999-12-31	aGVsbG8=
# line2	5678	0.5678	2000-01-01	d29ybGQ=
# '''.lstrip()
