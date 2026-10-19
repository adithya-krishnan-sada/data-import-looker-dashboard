-- # Copyright 2026 Google LLC
-- #
-- # Licensed under the Apache License, Version 2.0 (the "License");
-- # you may not use this file except in compliance with the License.
-- # You may obtain a copy of the License at
-- #
-- #     https://www.apache.org/licenses/LICENSE-2.0
-- #
-- # Unless required by applicable law or agreed to in writing, software
-- # distributed under the License is distributed on an "AS IS" BASIS,
-- # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- # See the License for the specific language governing permissions and
-- # limitations under the License.

-- Optional full reset (destructive). Uncomment only for a clean rebuild.
-- Dropping map_wave_details or the snapshots loses batch names and history older than 365 days.
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.map_execution_wave`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.map_wave_details`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_datatype_metrics`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_wave_metrics`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_execution_metrics`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_user_wave_metrics`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_user_overall_metrics`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_migration_errors`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_migration_timeline`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_top_errors`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.snapshot_item_user_wave`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.snapshot_user_wave`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.snapshot_migration_errors`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.snapshot_item_failures`;
-- DROP TABLE IF EXISTS `{project}.{target_dataset}.fact_failed_items`;

-- Base mapping table
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.map_execution_wave`(
  data_type STRING,
  migration_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  execution_id STRING,
  is_latest_execution BOOLEAN)
  CLUSTER BY data_type;

-- Unique wave details
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.map_wave_details`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING)
  CLUSTER BY data_type, batch_name;

-- fact_datatype_metrics
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_datatype_metrics`(
  data_type STRING,
  total_users_migrated INT64,
  successfully_migrated_items INT64,
  failed_items INT64,
  skipped_items INT64,
  warning_items INT64,
  total_discovered_items INT64,
  migrated_versions_count INT64,
  success_percentage FLOAT64)
  CLUSTER BY data_type;

-- fact_wave_metrics (Cumulative Batch Metrics)
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_wave_metrics`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  start_date TIMESTAMP,
  user_count INT64,
  completed_user_count INT64,
  successfully_migrated_items INT64,
  total_migrated_items INT64,
  total_discovered_items INT64,
  migrated_mails_count INT64,
  migrated_calendars_count INT64,
  migrated_contacts_count INT64,
  migrated_files_count INT64,
  migrated_folders_count INT64,
  skipped_items INT64,
  warning_items INT64,
  migrated_versions_count INT64,
  success_percentage FLOAT64,
  completion_percentage FLOAT64,
  avg_items_per_user FLOAT64,
  status STRING,
  outcome STRING)
  CLUSTER BY data_type, batch_id;

-- fact_execution_metrics (Per-Run Metrics)
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_execution_metrics`(
  execution_id STRING,
  run_start TIMESTAMP,
  is_latest_execution BOOLEAN,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  data_type STRING,
  successfully_migrated_items INT64,
  migrated_files_count INT64,
  migrated_folders_count INT64,
  failed_items INT64,
  skipped_items INT64,
  warning_items INT64,
  total_discovered_items INT64,
  migrated_versions_count INT64,
  success_percentage FLOAT64)
  CLUSTER BY data_type, batch_id;

-- fact_user_wave_metrics
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_user_wave_metrics`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  user_identifier STRING,
  total_items_migrated INT64,
  migrated_mails_count INT64,
  migrated_calendars_count INT64,
  migrated_contacts_count INT64,
  migrated_files_count INT64,
  migrated_folders_count INT64,
  failed_items INT64,
  skipped_items INT64,
  warning_items INT64,
  migrated_versions_count INT64,
  crawl_failure_items INT64,
  total_items INT64,
  total_discovered_items INT64,
  success_rate_percentage FLOAT64,
  status STRING,
  outcome STRING)
  CLUSTER BY data_type, batch_id;

-- fact_user_overall_metrics
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_user_overall_metrics`(
  user_identifier STRING,
  data_type STRING,
  batch_filter STRING,
  total_items_migrated INT64,
  migrated_mails_count INT64,
  migrated_calendars_count INT64,
  migrated_contacts_count INT64,
  migrated_files_count INT64,
  migrated_folders_count INT64,
  failed_items INT64,
  skipped_items INT64,
  warning_items INT64,
  migrated_versions_count INT64,
  total_items INT64,
  total_discovered_items INT64,
  success_rate_percentage FLOAT64,
  status STRING,
  outcome STRING)
  CLUSTER BY data_type;

-- fact_migration_errors
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_migration_errors`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  migration_error_code STRING,
  migration_error_title STRING,
  error_message STRING,
  user_identifier STRING,
  occurrence_count INT64)
  CLUSTER BY data_type, batch_id;

-- fact_top_errors
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_top_errors`(
  data_type STRING,
  migration_error_title STRING,
  error_message STRING,
  user_identifier STRING,
  occurrence_count INT64)
  CLUSTER BY data_type, migration_error_title;

-- fact_migration_timeline
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_migration_timeline`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  migration_date DATE,
  items_migrated INT64)
  CLUSTER BY data_type, batch_id;

-- Persistent snapshot for item statuses per batch
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.snapshot_item_user_wave`(
  data_type STRING,
  batch_id STRING,
  user_identifier STRING,
  source_identifier STRING,
  item_key STRING,
  item_class STRING,
  item_uuid STRING,
  event_name STRING,
  source_type STRING,
  event_status STRING,
  time_usec INT64,
  execution_id STRING)
  CLUSTER BY data_type, batch_id;

-- Persistent snapshot for user execution and completion state per batch
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.snapshot_user_wave`(
  data_type STRING,
  batch_id STRING,
  user_identifier STRING,
  start_time_usec INT64,
  is_completed BOOLEAN,
  latest_execution_id STRING,
  latest_execution_time_usec INT64)
  CLUSTER BY data_type, batch_id;

-- Persistent snapshot for unique migration error events
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.snapshot_migration_errors`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  migration_error_code STRING,
  migration_error_title STRING,
  error_message STRING,
  user_identifier STRING,
  execution_id STRING,
  time_usec INT64,
  event_uuid STRING)
  CLUSTER BY data_type, batch_id;

-- Persistent history of failed item events
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.snapshot_item_failures`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  execution_id STRING,
  user_identifier STRING,
  event_name STRING,
  source_type STRING,
  source_identifier STRING,
  source_uri STRING,
  migration_error_code STRING,
  migration_error_title STRING,
  error_message STRING,
  time_usec INT64,
  event_uuid STRING)
  CLUSTER BY data_type, batch_id;

-- fact_failed_items (one row per failed item with its first and latest error)
CREATE TABLE IF NOT EXISTS `{project}.{target_dataset}.fact_failed_items`(
  data_type STRING,
  batch_id STRING,
  batch_name STRING,
  batch_filter STRING,
  user_identifier STRING,
  source_type STRING,
  source_identifier STRING,
  source_uri STRING,
  initial_execution_id STRING,
  initial_time_usec INT64,
  initial_migration_error_code STRING,
  initial_migration_error_title STRING,
  initial_error_message STRING,
  latest_execution_id STRING,
  latest_time_usec INT64,
  latest_status STRING,
  latest_migration_error_code STRING,
  latest_migration_error_title STRING,
  latest_error_message STRING,
  resolution_status STRING)
  CLUSTER BY data_type, batch_id, resolution_status;

-- Add new columns to tables created by earlier versions
ALTER TABLE `{project}.{target_dataset}.fact_datatype_metrics` ADD COLUMN IF NOT EXISTS total_discovered_items INT64;
ALTER TABLE `{project}.{target_dataset}.fact_wave_metrics` ADD COLUMN IF NOT EXISTS total_discovered_items INT64;
ALTER TABLE `{project}.{target_dataset}.fact_execution_metrics` ADD COLUMN IF NOT EXISTS total_discovered_items INT64;
ALTER TABLE `{project}.{target_dataset}.fact_user_wave_metrics` ADD COLUMN IF NOT EXISTS total_discovered_items INT64;
ALTER TABLE `{project}.{target_dataset}.fact_user_overall_metrics` ADD COLUMN IF NOT EXISTS total_discovered_items INT64;
ALTER TABLE `{project}.{target_dataset}.fact_top_errors` ADD COLUMN IF NOT EXISTS data_type STRING;
ALTER TABLE `{project}.{target_dataset}.fact_execution_metrics` ADD COLUMN IF NOT EXISTS run_start TIMESTAMP;
ALTER TABLE `{project}.{target_dataset}.fact_execution_metrics` ADD COLUMN IF NOT EXISTS is_latest_execution BOOLEAN;
ALTER TABLE `{project}.{target_dataset}.map_execution_wave` ADD COLUMN IF NOT EXISTS migration_type STRING;
ALTER TABLE `{project}.{target_dataset}.fact_wave_metrics` ADD COLUMN IF NOT EXISTS outcome STRING;
ALTER TABLE `{project}.{target_dataset}.fact_user_wave_metrics` ADD COLUMN IF NOT EXISTS outcome STRING;
ALTER TABLE `{project}.{target_dataset}.fact_user_overall_metrics` ADD COLUMN IF NOT EXISTS outcome STRING;