#!/bin/bash

result=$(bq query --format=prettyjson --nouse_legacy_sql \
'SELECT
        date(detected_at) as date
        , database_name
        , schema_name
        , table_name
        , sub_type
        , ARRAY_AGG(column_name) as columns
 FROM
   `hubble-261722`.elementary.alerts_schema_changes
    GROUP BY
    1, 2, 3, 4, 5
    ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 ASC
')

echo "# Changes in data schema"

echo ""

echo "| Date       | Database Name | Schema Name | Table Name                     | Sub Type     | Columns                  |"
echo "|------------|---------------|-------------|--------------------------------|--------------|--------------------------|"

echo "$result" | jq -r '.[] | "| \(.date) | \(.database_name) | \(.schema_name) | \(.table_name) | \(.sub_type) | \(.columns | join(", ")) |"'
echo ""
