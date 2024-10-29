#!/bin/bash

result=$(bq query --format=prettyjson --nouse_legacy_sql \
    'SELECT
        date(detected_at) as date
        , table_name
        , sub_type as operation
        , ARRAY_AGG(column_name) as columns
 FROM
   `hubble-261722`.elementary.alerts_schema_changes
    GROUP BY
    1, 2, 3
    ORDER BY 1 DESC, 2 ASC
')

echo "# Changes in DBT marts schema"

echo ""

echo "| Date       |       Table Name                | Operation     | Columns                  |"
echo "|------------|---------------------------------|---------------|--------------------------|"

echo "$result" | jq -r '.[] | 
  "\(.date) | \(.table_name) | \(.operation) | \(.columns | join(", "))" ' | while IFS= read -r line; do
    IFS='|' read -ra fields <<< "$line"
    printf "| %-10s | %-60s | %-15s | %s |\n" "${fields[0]}" "${fields[1]}" "${fields[2]}" "${fields[3]}"
done
echo ""
