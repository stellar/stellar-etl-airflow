#!/bin/bash

project=$PROJECT
result=$(bq query --format=prettyjson --nouse_legacy_sql \
    "SELECT
        date(detected_at) as date
        , table_name
        , sub_type as operation
        , ARRAY_AGG(column_name) as columns
 FROM
    ${project}.elementary.alerts_schema_changes
    GROUP BY
    1, 2, 3
    ORDER BY 1 DESC, 2 ASC
")

echo "# Changes in DBT marts schema"

echo ""

echo "| Date       |       Table Name                | Operation     | Columns                  |"
echo "|------------|---------------------------------|---------------|--------------------------|"

echo "$result" | jq -r '.[] | "| \(.date) | \(.table_name ) | \(.operation) | \(.columns | join(", ")) |"'
echo ""
