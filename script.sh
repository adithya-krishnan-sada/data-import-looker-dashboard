# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# Function to handle failures
handle_failure() {
    local error_msg="$1"
    echo ""
    echo "❌ Error: $error_msg"
    echo ""
    while true; do
        read -p "Do you want to (r)etry from the start or (e)xit? [r/e]: " choice
        case "$choice" in
            [Rr]* ) 
                echo "🔄 Restarting deployment..."
                echo ""
                return 0
                ;;
            [Ee]* ) 
                echo "👋 Exiting."
                exit 1
                ;;
            * ) 
                echo "Please enter 'r' to retry or 'e' to exit."
                ;;
        esac
    done
}

echo "=========================================================="
echo "      DMS Import ANALYTICS - AUTOMATED DEPLOYMENT      "
echo "=========================================================="
echo ""

# ----------------------------------------------------------
# Disclaimer and Terms Acknowledgment
# ----------------------------------------------------------
cat << 'EOF'
Project Overview: The Exchange Online Data Import Analytics Dashboard project provides an automated deployment solution for monitoring of large-scale migrations using BigQuery and Looker Studio.

Data Accuracy Notice: The dashboard is offered "AS-IS" as an experimental feature and should not be relied on for business outcomes or hard migration timelines. The accuracy of the dashboard may vary and relies on, among other things, a correct BigQuery export setup and the underlying data within the customer's instance. The customer maintains full control over this configuration and data, the dashboard cannot guarantee migration statistics.

Monetary Cost Warning: Executing the deployment script provisions resources within your Google Cloud Project that incur real-time costs. By using these tools, you acknowledge sole responsibility for all financial obligations resulting from Google Cloud Platform.
EOF

echo ""
while true; do
    read -p "Please select 'Y' to acknowledge the terms in the LICENSE and README and  move forward or 'N' to exit: " ack_choice
    case "$ack_choice" in
        [Yy]* )
            echo "Acknowledged. Proceeding..."
            echo ""
            break
            ;;
        [Nn]* )
            echo "Exiting deployment."
            exit 0
            ;;
        * )
            echo "Invalid selection. Please enter 'Y' to acknowledge and proceed, or 'N' to exit."
            ;;
    esac
done

# ----------------------------------------------------------
# 1. Inputs
# ----------------------------------------------------------
read -p "Enter your Google Cloud Project ID: " PROJECT_ID
while [[ -z "$PROJECT_ID" ]]; do
   echo "Project ID cannot be empty."
   read -p "Enter your Google Cloud Project ID: " PROJECT_ID
done
echo ""

echo "Select BigQuery Data Location."
echo "Examples: 'US' (Multi-region), 'EU' (Multi-region), 'europe-west1', 'us-central1'"
read -p "Location (Press Enter for default 'US'): " BQ_LOC_INPUT
BQ_LOCATION=${BQ_LOC_INPUT:-US}
echo ""

read -p "Enter Source Dataset Name containing the audit log export (Press Enter for default 'GWS_Audit'): " SOURCE_INPUT
SOURCE_DATASET=${SOURCE_INPUT:-GWS_Audit}
echo ""

read -p "Enter Target Dataset Name for the reporting tables (Press Enter to use '$SOURCE_DATASET'): " TARGET_INPUT
TARGET_DATASET=${TARGET_INPUT:-$SOURCE_DATASET}
echo ""

echo "Select how often the dashboard should refresh."
echo "Examples: 'every 1 hours', 'every 6 hours', 'every day 02:00'"
read -p "Enter Schedule (Press Enter for default 'every day 02:00'): " SCHEDULE_INPUT
SCHEDULE=${SCHEDULE_INPUT:-"every day 02:00"}
DISPLAY_NAME="Data Import Dashboard Refresh"
echo ""

while true; do
START_TIME=$(date +%s)

# ----------------------------------------------------------
# 2. Enable Required APIs
# ----------------------------------------------------------
echo "⚡ Enabling required Google Cloud APIs..."
if ! gcloud services enable bigqueryreservation.googleapis.com --project "$PROJECT_ID"; then
    handle_failure "Failed to enable bigqueryreservation.googleapis.com API. Please check your permissions." && continue
fi
if ! gcloud services enable bigquerydatatransfer.googleapis.com --project "$PROJECT_ID"; then
    handle_failure "Failed to enable bigquerydatatransfer.googleapis.com API. Please check your permissions." && continue
fi
echo ""

# ----------------------------------------------------------
# 3. Read and Process SQL
# ----------------------------------------------------------
if [[ ! -f "ddl.sql" ]] || [[ ! -f "dml.sql" ]]; then
    handle_failure "'ddl.sql' or 'dml.sql' not found. Please ensure they are in the same folder." && continue
fi

# Create the target dataset if it does not exist yet
if ! bq show --project_id="$PROJECT_ID" "${PROJECT_ID}:${TARGET_DATASET}" &> /dev/null < /dev/null; then
    echo "📁 Target dataset '$TARGET_DATASET' not found. Creating it in $BQ_LOCATION..."
    if ! bq mk --dataset --location="$BQ_LOCATION" --project_id="$PROJECT_ID" "${PROJECT_ID}:${TARGET_DATASET}"; then
        handle_failure "Failed to create target dataset '$TARGET_DATASET'." && continue
    fi
    echo ""
fi

echo "🚀 Processing DDL and DML SQL..."
echo ""

# Process DDL
DDL_QUERY=$(sed -e "s/{project}/${PROJECT_ID}/g" \
                 -e "s/{source_dataset}/${SOURCE_DATASET}/g" \
                 -e "s/{target_dataset}/${TARGET_DATASET}/g" \
                 ddl.sql)

# Process DML Backfill (uses 365 days)
DML_BACKFILL_QUERY=$(sed -e "s/{project}/${PROJECT_ID}/g" \
                 -e "s/{source_dataset}/${SOURCE_DATASET}/g" \
                 -e "s/{target_dataset}/${TARGET_DATASET}/g" \
                 -e "s/{lookback_days}/365/g" \
                 dml.sql)

# Process DML Scheduled (uses 2 days)
DML_SCHEDULED_QUERY=$(sed -e "s/{project}/${PROJECT_ID}/g" \
                 -e "s/{source_dataset}/${SOURCE_DATASET}/g" \
                 -e "s/{target_dataset}/${TARGET_DATASET}/g" \
                 -e "s/{lookback_days}/2/g" \
                 dml.sql)

# ----------------------------------------------------------
# 4. Initial Execution
# ----------------------------------------------------------
echo "🚀 Running DDL to create tables in BigQuery ($BQ_LOCATION)..."
echo "$DDL_QUERY" | bq query --location="$BQ_LOCATION" --use_legacy_sql=false --project_id="$PROJECT_ID" || { handle_failure "DDL execution failed." && continue; }

echo "🚀 Running initial DML Backfill to populate historical data..."
echo "$DML_BACKFILL_QUERY" | bq query --location="$BQ_LOCATION" --use_legacy_sql=false --project_id="$PROJECT_ID" || { handle_failure "DML execution failed." && continue; }
echo ""

# ----------------------------------------------------------
# 5. Create the Scheduled Query
# ----------------------------------------------------------
echo "⏱️ Setting up the Automated Schedule ($SCHEDULE)..."

# Safely package the multi-line SQL into a JSON parameter string using jq
if ! command -v jq &> /dev/null; then
    echo "⚠️ 'jq' is not installed. Skipping scheduled query creation. Please install 'jq' to enable automation."
else
    PARAMS=$(jq -n --arg q "$DML_SCHEDULED_QUERY" '{"query": $q}')

    # Delete existing scheduled query with same name if it exists
    echo "🔍 Checking for existing scheduled query with name '$DISPLAY_NAME'..."
    if ! EXISTING_CONFIGS=$(bq --format=json ls --transfer_config --transfer_location="$BQ_LOCATION" --filter="dataSourceIds:scheduled_query" --project_id="$PROJECT_ID"); then
        handle_failure "Failed to check existing scheduled queries. Please verify your permissions." && continue
    fi
    
    if [[ -n "$EXISTING_CONFIGS" ]]; then
        CONFIGS_TO_DELETE=$(echo "$EXISTING_CONFIGS" | jq -r --arg name "$DISPLAY_NAME" '.. | objects | select(.displayName == $name) | .name' 2>/dev/null)
        
        if [[ -n "$CONFIGS_TO_DELETE" && "$CONFIGS_TO_DELETE" != "null" ]]; then
            # Read matching configs line-by-line (compatible with Bash 3.2+ on macOS)
            while IFS= read -r config; do
                if [[ -n "$config" && "$config" != "null" ]]; then
                    echo "🗑️ Found existing scheduled query: $config. Deleting..."
                    if bq rm -f --transfer_config "$config" < /dev/null; then
                        echo "✅ Existing scheduled query deleted: $config"
                    else
                        echo "⚠️ Warning: Failed to delete existing scheduled query: $config. Attempting to proceed..."
                    fi
                fi
            done <<< "$CONFIGS_TO_DELETE"
        else
            echo "ℹ️ No existing scheduled query with name '$DISPLAY_NAME' found."
        fi
    else
         echo "ℹ️ No existing scheduled queries found."
    fi

    if bq mk \
      --transfer_config \
      --project_id="$PROJECT_ID" \
      --location="$BQ_LOCATION" \
      --display_name="$DISPLAY_NAME" \
      --data_source="scheduled_query" \
      --schedule="$SCHEDULE" \
      --params="$PARAMS" \
      --force; then
        echo "✅ Schedule created successfully!"
    else
        handle_failure "Failed to create scheduled query." && continue
    fi
fi
echo ""

# ----------------------------------------------------------
# 6. Generate Magic Link
# ----------------------------------------------------------
MASTER_REPORT_ID="af17bd5f-8a4b-48ea-a56a-8b264de9ba9b"

# Template data source alias -> BigQuery table in the target dataset
DATA_SOURCES="ds0:map_wave_details ds1:fact_datatype_metrics ds2:fact_wave_metrics ds3:fact_user_wave_metrics ds4:fact_migration_timeline ds5:fact_user_overall_metrics ds6:fact_migration_errors ds7:fact_top_errors ds51:fact_failed_items"

MAGIC_LINK="https://lookerstudio.google.com/reporting/create?c.reportId=$MASTER_REPORT_ID&r.reportName=Data%20Import%20Dashboard"
for ds in $DATA_SOURCES; do
    alias=${ds%%:*}
    table=${ds#*:}
    MAGIC_LINK+="&ds.$alias.connector=bigQuery&ds.$alias.type=TABLE&ds.$alias.projectId=$PROJECT_ID&ds.$alias.datasetId=$TARGET_DATASET&ds.$alias.tableId=$table&ds.$alias.keepDatasourceName=true"
done
echo "=========================================================="
echo "✅ DEPLOYMENT COMPLETE"
echo "=========================================================="
echo "📊 CLICK THIS LINK TO COPY YOUR DASHBOARD:"
echo "$MAGIC_LINK"
echo ""

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "⏱️ Total startup time: $DURATION seconds"

break
done