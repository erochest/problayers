#!/usr/bin/env python


from contextlib import closing
import getpass
import sys

import psycopg2


DB_CXN = {
        'user' : getpass.getuser(),
        'host' : 'lon.lib.virginia.edu'
        }


def is_gis_db(cxn, db_name):
    """This checks that the database has a geometry_columns table. """
    with closing(cxn.cursor()) as c:
        c.execute('''
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema='public'
                  AND table_name='geometry_columns';
                ''')
        return (c.fetchone()[0] == 1)


def missing_gis_tables(cxn, db_name):
    """\
    This checks that the tables listed in geometry_columns.f_table_name
    actually exist.
    """
    with closing(cxn.cursor()) as c:
        c.execute('''
            SELECT f_table_name
            FROM geometry_columns
            WHERE f_table_name NOT IN (
                SELECT distinct table_name
                FROM information_schema.tables
                WHERE table_schema='public'
            )
            ORDER BY f_table_name;
            ''')
        for (table_name,) in c:
            yield table_name


def main():
    dbs = []
    with closing(psycopg2.connect(database='postgres', **DB_CXN)) as cxn:
        with closing(cxn.cursor()) as c:
            c.execute('''
                SELECT datname FROM pg_database
                WHERE datistemplate=false;
                ''')
            dbs = [ db_name for (db_name,) in c ]

    for db_name in dbs:
        with closing(psycopg2.connect(database=db_name, **DB_CXN)) as cxn:
            if is_gis_db(cxn, db_name):
                for table_name in missing_gis_tables(cxn, db_name):
                    sys.stdout.write('{0}.{1}\n'.format(db_name, table_name))

if __name__ == '__main__':
    main()

