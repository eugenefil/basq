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

import pytest


# add path to adosql to PATH
os.environ['PATH'] = (
    path.dirname(path.dirname(__file__)) + ':' + os.environ['PATH']
)


def test_select_string(capfd):
    p = subprocess.Popen(
        ['adosql', 'vfp', 'vfpdb/db.dbc'],
        universal_newlines=True,
        stdin=subprocess.PIPE
    )
    p.communicate(
        "select * from string"
    )

    assert p.returncode == 0

    out, err = capfd.readouterr()
    assert '''
value\r
string1\r
'''.lstrip() == out


#     assert out == '''
# string	integer	number	date	binary
# line1	1234	12.34	1999-12-31	aGVsbG8=
# line2	5678	0.5678	2000-01-01	d29ybGQ=
# '''.lstrip()
