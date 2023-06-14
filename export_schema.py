import pymongo
import json
from addict import Dict
from datetime import datetime
import argparse
import copy
import pandas as pd

TABLES = {}


def remove_anomalies(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, datetime):
                obj[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, str):
                obj[key] = value.replace("'", "''")
            elif 'ObjectId' in str(type(value)):
                obj[key] = str(value)
            elif isinstance(value, (dict, list)):
                remove_anomalies(value)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, datetime):
                obj[i] = item.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(item, str):
                obj[i] = item.replace("'", "''")
            elif 'ObjectId' in str(type(item)):
                obj[i] = str(item)
            elif isinstance(item, (dict, list)):
                remove_anomalies(item)


def parse(record):
    current_table_fields = []
    for entry in record:
        if isinstance(record[entry], dict) or isinstance(record[entry], list):
            remove_anomalies(record[entry])
            current_table_fields.append(
                (entry, json.dumps(record[entry], default=str), 'json'))
        else:
            data = record[entry]
            type = ''
            if isinstance(data, str):
                type = 'TEXT'
            elif isinstance(data, datetime):
                type = 'TIMESTAMP'
                data = data.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(data, bool):
                if data:
                    data = '1'
                else:
                    data = '0'
                type = 'BOOLEAN'
            elif isinstance(data, float):
                type = 'DOUBLE'
            elif isinstance(data, int):
                type = 'INTEGER'
            else:
                data = str(data)
                type = 'TEXT'
            current_table_fields.append((entry, data, type))
    schema = [(e[0], e[2]) for e in current_table_fields]
    return (schema, current_table_fields)


def get_master_schema(schemas):
    master_schema = []
    for schema in schemas:
        if len(schema) > len(master_schema):
            master_schema = schema
    return master_schema


def create_schema(schema, collection_id, records):
    global TABLES
    column_names = [field[0] for field in schema]

    table_data = []
    for record in records:
        record_entry = [entry[1] for entry in record]
        table_data.append(record_entry)
    df = pd.DataFrame(table_data, columns=column_names)

    TABLES[collection_id] = df
    return


def create_nested_schema(parent_id, source_df_name, target_field):
    global TABLES
    source_df = TABLES[source_df_name]
    records = source_df[target_field].tolist()
    parent_ids = source_df[parent_id].values.tolist()
    schemas = []
    for record in records:
        try:
            if isinstance(record, str):
                record = json.loads(record)
            if record:
                keys = [key for key in record.keys()]
                schemas.append(keys)
        except:
            pass
    column_names = [f'{source_df_name}_{parent_id}'] + \
        get_master_schema(schemas)

    table_data = []
    for index, record in enumerate(records):
        if record:
            record_entry = []
            for column in column_names:
                try:
                    if isinstance(record, str):
                        record = json.loads(record)

                    if column in record.keys():
                        record_entry.append(record[column])
                    elif column == f'{source_df_name}_{parent_id}':
                        record_entry.append(parent_ids[index])
                    else:
                        record_entry.append('')
                except:
                    pass
            table_data.append(record_entry)

    df = pd.DataFrame(table_data, columns=column_names)
    TABLES[f'{source_df_name}_{target_field}'] = (df)
    return


def trigger(mongo_conn, mongo_db, mongo_collection):
    mongo_client = pymongo.MongoClient(mongo_conn)
    db = mongo_client[mongo_db]
    collection = db[mongo_collection]

    schemas = []
    results = []
    print("\nFetching data from MongoDB...")
    for record in collection.find():
        record = Dict(record)
        schema, result = parse(record)
        schemas.append(schema)
        results.append(result)
    master_schema = get_master_schema(schemas)
    create_schema(master_schema, mongo_collection, results)


def is_json_object(value):
    try:
        my_dict = copy.deepcopy(value)
        if isinstance(my_dict, str):
            my_dict = json.loads(value)
        if isinstance(my_dict, dict):
            return True
        else:

            return False
    except Exception as e:
        return False


def get_json_fields(table):
    df = TABLES[table]
    df = df.dropna()
    json_fields = []
    columns = df.columns
    for index, row in df.iterrows():
        for column in columns:
            if is_json_object(row[column]):
                json_fields.append(column)
    return list(set(json_fields))


def expand_fields(table_name, initial_id_check):
    target_field = get_json_fields(table_name)

    if target_field:
        parent_id = [
            column for column in TABLES[table_name].columns if 'id' in column.lower()][0]

        create_nested_schema(parent_id, table_name, target_field[0])
        TABLES[table_name] = TABLES[table_name].drop([target_field[0]], axis=1)
        return 1, f'{table_name}_{target_field[0]}'
    else:
        print(f"\nNo more fields in {table_name} to expand...")
        return -1, None


def expand(table_name):
    intermediate_table_names = [table_name]
    initial_id_check = True
    while intermediate_table_names != []:
        print(f"\nExpanding: {intermediate_table_names[0]}")
        status_code, created_table = expand_fields(
            intermediate_table_names[0], initial_id_check)
        initial_id_check = False
        if status_code == -1:
            intermediate_table_names.remove(intermediate_table_names[0])
        elif status_code == 1:
            intermediate_table_names.append(created_table)
    print("\nExpansion process completed!")


def get_sql_type(value):
    if is_json_object(value):
        return 'json'
    elif isinstance(value, datetime):
        return 'TIMESTAMP'
    elif isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, float):
        return 'DOUBLE'
    try:
        if isinstance(value, list):
            return 'json'
        if isinstance(eval(value), list):
            return 'json'
    except:
        pass
    try:
        int(value)
        return 'INTEGER'
    except:
        pass

    return 'TEXT'


def export_schema():
    ddl_file = open("mysql_schema.info", 'w')
    for table in TABLES:
        df = TABLES[table]
        df.to_csv(f'{table}.csv', index=False, date_format='%Y-%m-%d %H:%M:%S')
        df = df.dropna()
        columns = df.columns
        field_types = []
        for index, row in df.iterrows():
            for column in columns:
                field_types.append(get_sql_type(row[column]))
            break
        export_df = pd.DataFrame(
            list(zip(list(columns), field_types)), columns=['Field', 'Type'])
        export_df.to_excel(f'{table}_schema.xlsx', index=False)

        command = f'CREATE TABLE {table} ('

        for index, column in enumerate(columns):
            if column != columns[-1]:
                command += f'{column} {field_types[index]}, '
            else:
                command += f'{column} {field_types[index]});'

        ddl_file.write(command + '\n')
    ddl_file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mongo_conn', type=str,
                        help='MongoDB Connection String')
    parser.add_argument('mongo_database', type=str,
                        help='MongoDB Database Name')
    parser.add_argument('mongo_collection', type=str,
                        help='MongoDB Collection Name')

    args = parser.parse_args()
    trigger(args.mongo_conn, args.mongo_database, args.mongo_collection)
    expand(args.mongo_collection)

    export_schema()
