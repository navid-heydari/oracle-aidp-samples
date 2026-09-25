#!/usr/bin/env python3
"""Read-only Snowflake database inventory and migration manifest generator.

The program issues only SELECT, SHOW, DESCRIBE, GET_DDL, and COUNT(*) commands.
It never creates, changes, exports, or deletes objects in Snowflake. Results are
written to a local JSON file, including DDL needed to recreate objects elsewhere.
"""

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path

import snowflake.connector


def json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value) if value is not None else None


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def qualified_name(database, schema, name):
    return '.'.join(map(quote_identifier, (database, schema, name)))


def rows(cursor, sql, params=None):
    cursor.execute(sql, params or ())
    columns = [column[0].lower() for column in cursor.description]
    return [dict(zip(columns, map(json_value, row))) for row in cursor.fetchall()]


def scalar(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchone()[0]


def ddl(cursor, kind, name):
    try:
        return scalar(cursor, "SELECT GET_DDL(%s, %s)", (kind, name))
    except snowflake.connector.Error as error:
        return None, str(error)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account', default=os.getenv('SNOWFLAKE_ACCOUNT'), required=not os.getenv('SNOWFLAKE_ACCOUNT'))
    parser.add_argument('--user', default=os.getenv('SNOWFLAKE_USER'), required=not os.getenv('SNOWFLAKE_USER'))
    parser.add_argument('--password', default=os.getenv('SNOWFLAKE_PASSWORD'))
    parser.add_argument('--warehouse', default=os.getenv('SNOWFLAKE_WAREHOUSE'), required=not os.getenv('SNOWFLAKE_WAREHOUSE'))
    parser.add_argument('--role', default=os.getenv('SNOWFLAKE_ROLE'))
    parser.add_argument('--database', required=True)
    parser.add_argument('--output', type=Path, default=Path('snowflake_inventory.json'))
    arguments = parser.parse_args()

    connection_args = {
        'account': arguments.account,
        'user': arguments.user,
        'warehouse': arguments.warehouse,
        'role': arguments.role,
        'database': arguments.database,
    }
    if arguments.password:
        connection_args['password'] = arguments.password

    connection = snowflake.connector.connect(**connection_args)
    cursor = connection.cursor()
    try:
        database = arguments.database
        assets = rows(cursor, """
            SELECT table_catalog, table_schema, table_name, table_type, comment,
                   created, last_altered, row_count, bytes
            FROM information_schema.tables
            WHERE table_catalog = %s
              AND table_schema <> 'INFORMATION_SCHEMA'
            ORDER BY table_schema, table_type, table_name
        """, (database,))

        manifest = {
            'source_database': database,
            'generated_at_utc': datetime.utcnow().isoformat() + 'Z',
            'read_only': True,
            'assets': [],
            'foreign_keys': [],
            'object_dependencies': [],
            'warnings': [],
        }

        for asset in assets:
            fq_name = qualified_name(database, asset['table_schema'], asset['table_name'])
            asset_result = {
                **asset,
                'qualified_name': fq_name,
                'columns': rows(cursor, """
                    SELECT column_name, ordinal_position, data_type, is_nullable,
                           column_default, comment
                    FROM information_schema.columns
                    WHERE table_catalog = %s AND table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (database, asset['table_schema'], asset['table_name'])),
                'describe': rows(
                    cursor,
                    f'DESCRIBE {"TABLE" if asset["table_type"] == "BASE TABLE" else "VIEW"} {fq_name}',
                ),
            }
            try:
                asset_result['exact_record_count'] = scalar(cursor, f'SELECT COUNT(*) FROM {fq_name}')
            except snowflake.connector.Error as error:
                asset_result['exact_record_count_error'] = str(error)

            ddl_result = ddl(cursor, 'TABLE' if asset['table_type'] == 'BASE TABLE' else 'VIEW', fq_name)
            if isinstance(ddl_result, tuple):
                asset_result['ddl_error'] = ddl_result[1]
            else:
                asset_result['ddl'] = ddl_result
            manifest['assets'].append(asset_result)

        manifest['foreign_keys'] = rows(cursor, """
            SELECT fk.table_schema AS child_schema, fk.table_name AS child_table,
                   fk.column_name AS child_fk_column, pk.table_schema AS parent_schema,
                   pk.table_name AS parent_table,
                   pk.column_name AS parent_pk_or_unique_column,
                   rc.constraint_name AS foreign_key_name
            FROM information_schema.referential_constraints rc
            JOIN information_schema.table_constraints fk_constraint
              ON fk_constraint.constraint_catalog = rc.constraint_catalog
             AND fk_constraint.constraint_schema = rc.constraint_schema
             AND fk_constraint.constraint_name = rc.constraint_name
            JOIN information_schema.key_column_usage fk
              ON fk.constraint_catalog = fk_constraint.constraint_catalog
             AND fk.constraint_schema = fk_constraint.constraint_schema
             AND fk.constraint_name = fk_constraint.constraint_name
            JOIN information_schema.table_constraints pk_constraint
              ON pk_constraint.constraint_catalog = rc.unique_constraint_catalog
             AND pk_constraint.constraint_schema = rc.unique_constraint_schema
             AND pk_constraint.constraint_name = rc.unique_constraint_name
            JOIN information_schema.key_column_usage pk
              ON pk.constraint_catalog = pk_constraint.constraint_catalog
             AND pk.constraint_schema = pk_constraint.constraint_schema
             AND pk.constraint_name = pk_constraint.constraint_name
             AND pk.ordinal_position = fk.position_in_unique_constraint
            WHERE fk.table_catalog = %s
            ORDER BY child_schema, child_table, foreign_key_name, child_fk_column
        """, (database,))

        try:
            manifest['object_dependencies'] = rows(cursor, """
                SELECT referencing_object_domain, referencing_object_name,
                       referenced_object_domain, referenced_object_name, dependency_type
                FROM snowflake.account_usage.object_dependencies
                WHERE referencing_object_name ILIKE %s AND deleted IS NULL
                ORDER BY referencing_object_name, referenced_object_name
            """, (database + '.%',))
        except snowflake.connector.Error as error:
            manifest['warnings'].append(
                'View/object lineage was not collected: ' + str(error)
            )

        arguments.output.write_text(json.dumps(manifest, indent=2, default=json_value) + '\n')
        print(f'Wrote {len(manifest["assets"])} assets to {arguments.output.resolve()}')
    finally:
        cursor.close()
        connection.close()


if __name__ == '__main__':
    main()
