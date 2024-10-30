import os
import json
from datetime import datetime

old_schemas_dir = "original_schemas"
new_schemas_dir = "new_schemas"

if not os.path.exists(old_schemas_dir):
    print(f"Directory {old_schemas_dir} does not exist.")
    exit(1)

if not os.path.exists(new_schemas_dir):
    print(f"Directory {new_schemas_dir} does not exist.")
    exit(1)

def read_json_file(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

new_schemas = [file.replace("_schema.json", "") for file in os.listdir(new_schemas_dir)]
old_schemas = [file.replace("_schema.json", "") for file in os.listdir(old_schemas_dir)]

tables_added = [model for model in new_schemas if model not in old_schemas]
tables_removed = [model for model in old_schemas if model not in new_schemas]
common_tables = [model for model in new_schemas if model in old_schemas]

schema_changes = {}

for schema in common_tables:
    old_file_path = os.path.join(old_schemas_dir, schema + '_schema.json')
    new_file_path = os.path.join(new_schemas_dir, schema + '_schema.json')

    new_data = read_json_file(new_file_path)
    old_data = read_json_file(old_file_path)

    old_dict = {item['name']: item for item in old_data}
    new_dict = {item['name']: item for item in new_data}

    added = [new_dict[name]['name'] for name in new_dict if name not in old_dict]
    deleted = [old_dict[name]['name'] for name in old_dict if name not in new_dict]
    type_changed = [
        (new_dict[name]['name'], new_dict[name]['type'], old_dict[name]['type']) for name in new_dict 
        if name in old_dict and new_dict[name]['type'] != old_dict[name]['type']
    ]

    if added:
        if schema not in schema_changes:
            schema_changes[schema] = {}
        schema_changes[schema]['column_added'] = added

    if deleted:
        if schema not in schema_changes:
            schema_changes[schema] = {}
        schema_changes[schema]['column_removed'] = deleted

    if type_changed:
        if schema not in schema_changes:
            schema_changes[schema] = {}
        schema_changes[schema]['type_changed'] = type_changed

if tables_added or tables_removed or schema_changes:
    current_date = datetime.now().strftime("%Y-%m-%d")
    print(f"## {current_date}")

if tables_added:
    print("")
    print("## Tables Added:")
    print([table for table in tables_added])

if tables_removed:
    print("")
    print("### Tables Removed:")
    print([table for table in tables_removed])

def sort_schema_changes(changes):
    sorted_data = {}

    for table_name in sorted(changes.keys()):
        sorted_operations = {op_type: sorted(changes[table_name][op_type]) 
                            for op_type in sorted(changes[table_name].keys())}
        sorted_data[table_name] = sorted_operations
    return sorted_data

if schema_changes:
    sorted_schema_changes = sort_schema_changes(schema_changes)
    print("")
    print("### Schema Changes:")

    markdown_table = "|       Table Name                | Operation     | Columns                  |\n"
    markdown_table += "|---------------------------------|---------------|--------------------------|\n"

    for table_name, operations in sorted_schema_changes.items():
        for operation, columns in operations.items():
            if operation in ['column_added', 'column_removed']:
                columns_str = ", ".join(columns)
            if operation in ['type_changed']:
                columns_str = ", ".join([f"{column[0]} ({column[2]} -> {column[1]})" for column in columns])
            markdown_table += f"| {table_name:<33} | {operation:<13} | {columns_str:<24} |\n"
    print(markdown_table)
