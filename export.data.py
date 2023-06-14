import argparse
import pandas as pd
import mysql.connector

TABLES = {}
SCHEMAS = {}


def store_missed_records(query):
    try:
        file = open('failing_queries.log', 'a')
        query = str(query).replace('\n', '')
        file.write(f'DATA: {query}\n')
        file.close()
    except:
        pass


def execute_mysql_query(connector, cursor, query):
    try:
        cursor.execute(query)
        connector.commit()
    except:
        store_missed_records(query)
    return


def load_schema():
    global TABLES, SCHEMAS

    file = open('mysql_schema.info', 'r')
    lines = file.readlines()
    print("Fetched tables: ")
    for line in lines:
        table_name = line.split(' (')[0].split('TABLE ')[1]
        print(table_name)
        df = pd.read_csv(f'{table_name}.csv')
        TABLES[table_name] = df

        fields = line.split('(')[1].split(')')[0].split(', ')
        schema = []
        for field in fields:
            single_field = field.split()
            attribute = single_field[0]
            d_type = single_field[1]
            schema.append((attribute, d_type))
        SCHEMAS[table_name] = schema


def dump_data(connector, cursor):
    for table_name in SCHEMAS:
        query = f'CREATE TABLE {table_name} ('
        for object in SCHEMAS[table_name]:
            query += f'{object[0]} {object[1]},'
        query += ')'
        query = query.replace(",)", ");")
        execute_mysql_query(connector, cursor, query)

        df = TABLES[table_name]
        for index, row in df.iterrows():
            query = f'INSERT INTO {table_name} ('
            for object in SCHEMAS[table_name]:
                query += object[0] + ','
            query += ')'
            query = query.replace(',)', ") VALUES (")
            for object in SCHEMAS[table_name]:
                query += f"'{row[object[0]]}',"
            query += ')'
            query = query.replace(',)', ");")
            execute_mysql_query(connector, cursor, query)


def trigger(host, user, password, db):
    mysql_connector = mysql.connector.connect(user=user, password=password,
                                              host=host,
                                              database=db)
    mysql_cursor = mysql_connector.cursor()
    load_schema()
    dump_data(mysql_connector, mysql_cursor)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('host', type=str)
    parser.add_argument('username', type=str)
    parser.add_argument('password', type=str)
    parser.add_argument('database', type=str)

    args = parser.parse_args()
    trigger(args.host, args.username, args.password, args.database)
