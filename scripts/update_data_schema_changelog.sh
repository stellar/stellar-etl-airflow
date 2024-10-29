#!/bin/bash

# result=$(bq query --format=prettyjson --nouse_legacy_sql \
# 'SELECT
#         date(detected_at) as date
#         , database_name
#         , schema_name
#         , table_name
#         , sub_type
#         , ARRAY_AGG(column_name) as columns
#  FROM
#    `hubble-261722`.elementary.alerts_schema_changes
#     GROUP BY
#     1, 2, 3, 4, 5
#     ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 ASC
# ')

result=$(bq query --nouse_legacy_sql 'SELECT count(*) from `hubble-261722`.crypto_stellar_internal_2.country_code')
echo "# Changes in data schema"
echo "$result"

# echo ""

# echo "| Date       | Database Name | Schema Name | Table Name                     | Sub Type     | Columns                  |"
# echo "|------------|---------------|-------------|--------------------------------|--------------|--------------------------|"

# echo "$result" | jq -r '.[] | "| \(.date) | \(.database_name) | \(.schema_name) | \(.table_name) | \(.sub_type) | \(.columns | join(", ")) |"'
