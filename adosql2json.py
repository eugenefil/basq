#!/usr/bin/env winpython

# TODO remove -chop arg, use narg='?' with -trim
# TODO change "field" to "column" (sql talks about columns)

import sys
from collections import OrderedDict

import adodbapi
import argparse
import jsondate as json


ADO_FIELD_NAME = 0
ADO_FIELD_TYPE = 1
ADO_FIELD_SCALE = 5

CONN_STR_VFP = u'Provider=VFPOLEDB.1;Data Source={datasource};Mode=Share Deny None;Extended Properties="";User ID="";Mask Password=False;Cache Authentication=False;Encrypt Password=False;Collating Sequence=MACHINE;DSN="";DELETED=True;CODEPAGE=1251;MVCOUNT=16384;ENGINEBEHAVIOR=90;TABLEVALIDATE=3;REFRESH=5;VARCHARMAPPING=False;ANSI=True;REPROCESS=5'

CONN_STR_MSSQL = u'Provider=SQLNCLI10;Server={server};Database={database};{auth}'
CONN_STR_MSSQL_SRVAUTH = u'Uid={user};Pwd={password}'
CONN_STR_MSSQL_WINAUTH = u'Integrated Security=SSPI'


def to_unicode(s): return s.decode('windows-1251')
def unescape_and_to_unicode(s): return to_unicode(s.decode('string_escape'))


def cvtDate(defconv):
    def todate(comdate):
        return None if float(comdate) == 0.0 else defconv(comdate).date()
    return todate


def compose(*funcs):
    if len(funcs) > 0:
        return reduce(lambda f, g: lambda *args: f(g(*args)), funcs)


def cvtTrim(chars):
    def trim(unic): return unic.strip(chars)
    return trim


def cvtRTrim(chars):
    def trim(unic): return unic.rstrip(chars)
    return trim


def cvtReplace(pairs):
    def replace(unic):
        for old, new in pairs:
            unic = unic.replace(old, new)
        return unic
    return replace


ADO_INTEGER = adodbapi.apibase.DBAPITypeObject(
    adodbapi.apibase.adoIntegerTypes +
    adodbapi.apibase.adoLongTypes)

def ado_type_to_general(flddesc):
    adotype = flddesc[ADO_FIELD_TYPE]
    gentype = ''
    if adotype == ADO_INTEGER:
        gentype = 'int'
    elif adotype == adodbapi.NUMBER:
        # Number with zero decimals is an integer.
        gentype = 'int' if flddesc[ADO_FIELD_SCALE] == 0 else 'float'
    elif adotype == adodbapi.STRING:
        gentype = 'text'
    elif adotype == adodbapi.DATETIME:
        gentype = 'date'
    else:
        raise TypeError('ADO type %s cannot be converted to general type' % adotype)
    return gentype


def sqlcmdtojson(cmd, cursor, type_converters={}, rowtype='dict'):
    cursor.execute(cmd)
    # sys.stderr.write("total rows: %d\n" % cursor.rowcount)
    # sys.stderr.write(repr(cursor.description) + '\n')

    fields = []
    for i, desc in enumerate(cursor.description):
        fldtype = ado_type_to_general(desc)
        fields.append({'name': desc[ADO_FIELD_NAME], 'type': fldtype})

        if fldtype in type_converters:
            cursor.converters[i] = type_converters[fldtype]
    # sys.stderr.write(repr(fields) + '\n')

    if rowtype == 'list':
        rows = [list(row) for row in cursor]
        obj = {'fields': fields, 'rows': rows}
    else:
        fldnames = [f['name'] for f in fields]
        obj = [OrderedDict(zip(fldnames, list(row))) for row in cursor]
    return json.dumps(obj, indent=2)


def main(connstr, rowtype, trim, chop, replace):
    # sys.stderr.write(connstr + '\n')
    conn = adodbapi.connect(connstr)

    # Set proper connection-wide db-to-python type conversions.
    conn.variantConversions = adodbapi.apibase.variantConversions
    # Convert numeric to float.
    conn.variantConversions[adodbapi.ado_consts.adNumeric] = adodbapi.apibase.cvtFloat
    # Convert empty dates to None, truncate time info (it is empty).
    conn.variantConversions[adodbapi.ado_consts.adDBDate] = cvtDate(
        conn.variantConversions[adodbapi.ado_consts.adDBDate])

    replace_pairs = [[s[0], s[1:]] for s in replace]

    # Build filter chain for string conversion.
    strfilters = map(lambda on, getter: getter() if on else None,
                     [trim, replace, True],
                     [lambda: trim(chop),
                      lambda: cvtReplace(replace_pairs),
                      lambda: adodbapi.apibase.cvtUnicode])
    cvtStr = compose(*filter(None, strfilters))
    #sys.stderr.write(repr(strfilters) + '\n')

    type_converters = {
        'int': adodbapi.apibase.cvtInt,
        'text': cvtStr
    }

    cur = conn.cursor()
    for sqlcmd in sys.stdin:
        jsonstr = sqlcmdtojson(
            sqlcmd.decode('utf-8'),
            cur,
            type_converters=type_converters,
            rowtype=rowtype
        )
        sys.stdout.write(jsonstr + '\n')

    cur.close()
    conn.close()


def vfpconnstr(args):
    return CONN_STR_VFP.format(datasource=args.database)


def mssqlconnstr(args):
    auth = CONN_STR_MSSQL_WINAUTH
    if args.user:
        auth = CONN_STR_MSSQL_SRVAUTH.format(user=args.user,
                                             password=args.password)
    return CONN_STR_MSSQL.format(auth=auth,
                                 server=args.server,
                                 database=args.database)


def trim_action(trimfunc):
    class TrimAction(argparse.Action):
        def __init__(self, *args, **kwargs):
            kwargs['nargs'] = 0
            super(TrimAction, self).__init__(*args, **kwargs)
            self.trimfunc = trimfunc

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, 'trim', self.trimfunc)

    return TrimAction


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-rowtype',
        default='list',
        choices=['list', 'dict'],
        help='represent each data row as list or dict. If list (default), field names and types are stored in separate dict'
    )
    parser.add_argument(
        '-trim',
        action=trim_action(cvtTrim),
        help='trim leading and trailing characters'
    )
    parser.add_argument(
        '-rtrim',
        action=trim_action(cvtRTrim),
        help='trim trailing characters'
    )
    parser.add_argument(
        '-chop',
        default=' ',
        type=unescape_and_to_unicode,
        help="characters to trim. Default: ' '",
        metavar='CHARS'
    )
    parser.add_argument(
        '-replace',
        action='append',
        default=[],
        type=unescape_and_to_unicode,
        help='replace first character with rest of string',
        metavar='STR'
    )
    subparsers = parser.add_subparsers(title='commands')

    vfpparser = subparsers.add_parser(
        'vfp',
        help='Visual FoxPro'
    )
    vfpparser.add_argument(
        'database',
        type=to_unicode
    )
    vfpparser.set_defaults(getconnstr=vfpconnstr)

    mssqlparser = subparsers.add_parser(
        'mssql',
        description='Windows Authentication is used by default',
        help='Microsoft SQL Server'
    )
    mssqlparser.add_argument(
        '-server',
        default='localhost',
        type=to_unicode,
        help='server ip/name. Default: localhost',
        metavar='SRV'
    )
    mssqlparser.add_argument(
        '-database',
        default='',
        type=to_unicode,
        help='initial database. Default: server settings',
        metavar='DB'
    )
    mssqlparser.add_argument(
        '-user',
        type=to_unicode,
        help='user name for SQL Server Authentication'
    )
    mssqlparser.add_argument(
        '-password',
        default='',
        type=to_unicode,
        metavar='PASS'
    )
    mssqlparser.set_defaults(getconnstr=mssqlconnstr)
    
    args = parser.parse_args()
    main(args.getconnstr(args),
         args.rowtype,
         args.trim,
         args.chop,
         args.replace)
