import json
import os
import sys
from datetime import datetime

OLD_SCHEMAS_DIR = os.environ.get("OLD_SCHEMAS_DIR")
OLD_CHANGELOG_FILEPATH = os.environ.get("OLD_CHANGELOG_FILEPATH")
NEW_SCHEMAS_DIR = "schemas"
CHANGELOG_FILEPATH = "changelog/source_data.md"


def read_json_file(filepath: str) -> {}:
    with open(filepath, "r") as rfp:
        try:
            return json.load(rfp)
        except:
            sys.exit(f"Not a valid JSON at filepath {filepath}.")


def read_file(filepath: str) -> str:
    with open(filepath, "r") as rfp:
        return rfp.read()


def write_file(filepath: str, content: str, mode="a") -> None:
    with open(filepath, mode) as wfp:
        wfp.write(content)


def sort_schema_changes(schema_changes: {}) -> {}:
    sorted_data = {}

    for table_name, op_types in sorted(schema_changes.items()):
        sorted_data[table_name] = {
            op_type: sorted(columns) for op_type, columns in sorted(op_types.items())
        }
    return sorted_data


def list_files_in_dir(directory: str) -> []:
    if not os.path.exists(directory):
        sys.exit(f"Directory {directory} does not exist.")
    return os.listdir(directory)


def compare_lists(old_list=[], new_list=[]):
    old_set = set(old_list)
    new_set = set(new_list)

    common = old_set & new_set
    added = new_set - old_set
    deleted = old_set - new_set

    return list(common), list(added), list(deleted)


def get_mapped_schema_json(directory: str, schema_name: str) -> {}:
    """
    Returns schema object indexed by field name.
    Example:
    {
        "field_name_1": {
            "name": field_name_1,
            "type": STRING
        },
        "field_name_2": {
            "name": field_name_2,
            "type": STRING
        },
    }
    """
    schema_json = read_json_file(os.path.join(directory, schema_name))
    schema_json_by_col_name = {column["name"]: column for column in schema_json}
    return schema_json_by_col_name


def compare_schemas(schemas=[]) -> {}:
    schema_changes = {}

    for schema in schemas:
        old_schema_json_by_col_name = get_mapped_schema_json(
            directory=OLD_SCHEMAS_DIR, schema_name=schema
        )
        new_schema_json_by_col_name = get_mapped_schema_json(
            directory=NEW_SCHEMAS_DIR, schema_name=schema
        )

        common, added, deleted = compare_lists(
            old_list=old_schema_json_by_col_name.keys(),
            new_list=new_schema_json_by_col_name.keys(),
        )

        type_changed = [
            (
                col,
                old_schema_json_by_col_name[col]["type"],
                new_schema_json_by_col_name[col]["type"],
            )
            for col in common
            if old_schema_json_by_col_name[col]["type"]
            != new_schema_json_by_col_name[col]["type"]
        ]

        if added or deleted or type_changed:
            schema_changes[schema] = {
                "column_added": added,
                "column_removed": deleted,
                "type_changed": type_changed,
            }

    return schema_changes


def get_print_label_string(label: str) -> str:
    return f"\n## {label}:\n" if label else label


def get_print_schemas_string(label="", schemas=[]) -> str:
    print_string = ""
    if not len(schemas):
        return print_string

    print_string += get_print_label_string(label)
    for schema in schemas:
        print_string += f"- {schema.replace('_schema.json', '')}"
    return print_string


def get_print_schema_changes_string(label="", schema_changes={}) -> str:
    print_string = ""
    if not schema_changes:
        return print_string

    print_string += get_print_label_string(label)

    markdown_table = "|       Table Name                | Operation     | Columns                                            |\n"
    markdown_table += "|---------------------------------|---------------|----------------------------------------------------|\n"

    for schema_name, operations in schema_changes.items():
        for operation, columns in operations.items():
            if len(columns):
                if operation in ["column_added", "column_removed"]:
                    columns_str = ", ".join(columns)
                if operation in ["type_changed"]:
                    columns_str = ", ".join(
                        [
                            f"{column[0]} ({column[1]} -> {column[2]})"
                            for column in columns
                        ]
                    )
                markdown_table += f"| {get_print_schemas_string(schemas=[schema_name]):<31} | {operation:<13} | {columns_str:<48} |\n"
    print_string += markdown_table
    return print_string


def generate_changelog(schemas_added=[], schemas_deleted=[], schemas_changes={}) -> str:
    new_changelog = ""
    if schemas_added or schemas_deleted or schemas_changes:
        current_date = datetime.now().strftime("%Y-%m-%d")
        new_changelog += get_print_label_string(current_date)

    new_changelog += get_print_schemas_string(
        label="Tables Added", schemas=schemas_added
    )
    new_changelog += get_print_schemas_string(
        label="Tables Deleted", schemas=schemas_deleted
    )

    sorted_schema_changes = sort_schema_changes(schemas_changes)
    new_changelog += get_print_schema_changes_string(
        label="Schema Changes", schema_changes=sorted_schema_changes
    )
    return new_changelog


def main():
    existing_changelog = read_file(filepath=OLD_CHANGELOG_FILEPATH)
    old_schema_filepaths = list_files_in_dir(directory=OLD_SCHEMAS_DIR)
    new_schema_filepaths = list_files_in_dir(directory=NEW_SCHEMAS_DIR)

    common, added, deleted = compare_lists(
        old_list=old_schema_filepaths, new_list=new_schema_filepaths
    )
    schema_changes = compare_schemas(common)
    new_changelog = generate_changelog(
        schemas_added=added, schemas_deleted=deleted, schemas_changes=schema_changes
    )

    if len(new_changelog):
        # TODO: Append to same date if multiple changelog commited in same day
        write_file(
            filepath=CHANGELOG_FILEPATH, mode="w", content=new_changelog + "\n\n"
        )
        write_file(filepath=CHANGELOG_FILEPATH, mode="a", content=existing_changelog)
    else:
        # Rewrite the changelog since intermediate commits may have overwritten changelog
        write_file(filepath=CHANGELOG_FILEPATH, mode="w", content=existing_changelog)


if __name__ == "__main__":
    main()
