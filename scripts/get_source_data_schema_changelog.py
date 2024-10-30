import os
import json

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

if tables_added:
    print("")
    print("## Tables Added:")
    print([table for table in tables_added])

if tables_removed:
    print("")
    print("## Tables Removed:")
    print([table for table in tables_removed])

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

    if added or deleted or type_changed:
        print("")
        print(f"## {schema}")

    if added:
        print(f'**Added columns:** {[field for field in added]}')

    if deleted:
        print(f'**Deleted columns:** {[field for field in deleted]}')

    if type_changed:
        for (field, new_type, old_type) in type_changed:
            print(f'Type changed for column {field} from {old_type} to {new_type}')
