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

-- Step 1: Incremental Window Materialization ({lookback_days} DAY Lookback)
CREATE OR REPLACE TEMP TABLE incremental_logs
AS
SELECT
  time_usec,
  record_type,
  event_name,
  event_type,
  status,
  data_migration
FROM
  `{project}.{source_dataset}.activity`
WHERE
  record_type = 'data_migration'
  AND data_migration.migration_type IN (
    'Exchange Online Migration',
    'SharePoint Online Enterprise Migration',
    'SharePoint Online Migration',
    'Microsoft OneDrive Migration'
  )
  AND _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {lookback_days} DAY);

-- Step 2: Latest Wave Names Extraction
CREATE OR REPLACE TEMP TABLE latest_wave_names AS
SELECT
  REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)') AS wave_id,
  IFNULL(NULLIF(data_migration.target_identifier, ''), 'Default Import Batch') AS batch_name,
  time_usec
FROM incremental_logs
WHERE
  REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)') IS NOT NULL
  AND (
    (event_name = 'START_MIGRATION_SETUP')
    OR (
      event_name = 'UPDATE_MIGRATION_SETTINGS'
      AND data_migration.target_identifier IS NOT NULL
      AND data_migration.target_identifier != ''
    )
  )
QUALIFY
  ROW_NUMBER() OVER (
      PARTITION BY REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)')
      ORDER BY time_usec DESC
    ) = 1;

-- Step 3: Session Mapping Generation
-- 3a: One row per execution with its raw migration_type
CREATE OR REPLACE TEMP TABLE current_wave_map_raw
AS
WITH
  StartEvents AS (
    SELECT
      data_migration.migration_type AS migration_type,
      REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)') AS wave_id,
      data_migration.execution_id AS execution_id,
      ROW_NUMBER() OVER (
          PARTITION BY REGEXP_EXTRACT(data_migration.target_uri, r'WaveId: ([\w-]+)')
          ORDER BY time_usec DESC
        ) = 1 AS is_latest_execution
    FROM incremental_logs
    WHERE event_name = 'START_MIGRATION'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data_migration.execution_id ORDER BY time_usec DESC) = 1
  ),
  HistoricalDetails AS (
    SELECT batch_id AS wave_id, ANY_VALUE(batch_name) AS batch_name
    FROM `{project}.{target_dataset}.map_wave_details`
    GROUP BY batch_id
  )
SELECT
  e.migration_type,
  e.wave_id AS batch_id,
  COALESCE(s.batch_name, h.batch_name, 'Default Migration Batch') AS batch_name,
  CONCAT(COALESCE(s.batch_name, h.batch_name, 'Default Migration Batch'), " (ID: ", CAST(e.wave_id AS STRING), ")") AS batch_filter,
  e.execution_id,
  e.is_latest_execution
FROM StartEvents e
LEFT JOIN latest_wave_names s ON e.wave_id = s.wave_id
LEFT JOIN HistoricalDetails h ON e.wave_id = h.wave_id;

-- 3b: Resolve data_type per batch
-- 'SharePoint Online Enterprise Migration' is split into SharePoint Online / OneDrive by the majority of item source URLs.
CREATE OR REPLACE TEMP TABLE batch_type_resolved AS
WITH
  BatchExec AS (
    SELECT batch_id, execution_id FROM current_wave_map_raw WHERE batch_id IS NOT NULL
    UNION DISTINCT
    SELECT batch_id, execution_id FROM `{project}.{target_dataset}.map_execution_wave` WHERE batch_id IS NOT NULL
  ),
  BatchRaw AS (
    SELECT
      batch_id,
      ARRAY_AGG(raw_type IGNORE NULLS ORDER BY prio LIMIT 1)[SAFE_OFFSET(0)] AS raw_type,
      ARRAY_AGG(IF(existing_type IN ('Exchange Online', 'SharePoint Online', 'OneDrive'), existing_type, NULL) IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS existing_type
    FROM (
      SELECT batch_id, migration_type AS raw_type, CAST(NULL AS STRING) AS existing_type, 0 AS prio
      FROM current_wave_map_raw WHERE batch_id IS NOT NULL
      UNION ALL
      SELECT batch_id, COALESCE(migration_type, data_type) AS raw_type, data_type AS existing_type, 1 AS prio
      FROM `{project}.{target_dataset}.map_execution_wave` WHERE batch_id IS NOT NULL
    )
    GROUP BY batch_id
  ),
  WindowVotes AS (
    SELECT x.batch_id, COUNTIF(v.is_od) AS od, COUNTIF(NOT v.is_od) AS sp
    FROM BatchExec x
    JOIN (
      SELECT
        data_migration.execution_id AS execution_id,
        REGEXP_CONTAINS(COALESCE(data_migration.source_name, data_migration.source_uri), r'-my\.sharepoint\.com|/personal/') AS is_od
      FROM incremental_logs
      WHERE LOWER(event_type) = 'migration'
        AND (data_migration.source_type LIKE 'Sharepoint%' OR data_migration.source_type LIKE 'OneDrive%')
        AND COALESCE(data_migration.source_name, data_migration.source_uri) IS NOT NULL
    ) v ON v.execution_id = x.execution_id
    GROUP BY x.batch_id
  ),
  SnapshotVotes AS (
    SELECT
      batch_id,
      COUNTIF(REGEXP_CONTAINS(user_identifier, r'-my\.sharepoint\.com|/personal/')) AS od,
      COUNTIF(user_identifier != '(unknown)' AND NOT REGEXP_CONTAINS(user_identifier, r'-my\.sharepoint\.com|/personal/')) AS sp
    FROM `{project}.{target_dataset}.snapshot_item_user_wave`
    GROUP BY batch_id
  )
SELECT
  r.batch_id,
  r.raw_type AS migration_type,
  CASE r.raw_type
    WHEN 'Exchange Online Migration' THEN 'Exchange Online'
    WHEN 'SharePoint Online Migration' THEN 'SharePoint Online'
    WHEN 'Microsoft OneDrive Migration' THEN 'OneDrive'
    WHEN 'SharePoint Online Enterprise Migration' THEN
      CASE
        WHEN IFNULL(v.od, 0) + IFNULL(v.sp, 0) + IFNULL(s.od, 0) + IFNULL(s.sp, 0) = 0
          THEN COALESCE(r.existing_type, 'SharePoint Online')
        WHEN IFNULL(v.od, 0) + IFNULL(s.od, 0) > IFNULL(v.sp, 0) + IFNULL(s.sp, 0) THEN 'OneDrive'
        ELSE 'SharePoint Online'
      END
    ELSE r.raw_type
  END AS data_type
FROM BatchRaw r
LEFT JOIN WindowVotes v ON r.batch_id = v.batch_id
LEFT JOIN SnapshotVotes s ON r.batch_id = s.batch_id;

-- 3c: Batches whose data_type label changed
CREATE OR REPLACE TEMP TABLE batch_type_changes AS
SELECT DISTINCT r.batch_id, x.data_type AS old_data_type, r.data_type AS new_data_type
FROM batch_type_resolved r
JOIN (
  SELECT DISTINCT batch_id, data_type FROM `{project}.{target_dataset}.map_execution_wave`
  UNION DISTINCT SELECT DISTINCT batch_id, data_type FROM `{project}.{target_dataset}.snapshot_user_wave`
  UNION DISTINCT SELECT DISTINCT batch_id, data_type FROM `{project}.{target_dataset}.snapshot_item_user_wave`
  UNION DISTINCT SELECT DISTINCT batch_id, data_type FROM `{project}.{target_dataset}.fact_wave_metrics`
) x ON x.batch_id = r.batch_id
WHERE x.data_type IS DISTINCT FROM r.data_type;

-- 3d: Final session map
CREATE OR REPLACE TEMP TABLE current_wave_map
AS
SELECT
  COALESCE(
    b.data_type,
    CASE c.migration_type
      WHEN 'Exchange Online Migration' THEN 'Exchange Online'
      WHEN 'SharePoint Online Migration' THEN 'SharePoint Online'
      WHEN 'Microsoft OneDrive Migration' THEN 'OneDrive'
      WHEN 'SharePoint Online Enterprise Migration' THEN 'SharePoint Online'
      ELSE c.migration_type
    END) AS data_type,
  c.migration_type, c.batch_id, c.batch_name, c.batch_filter, c.execution_id, c.is_latest_execution
FROM current_wave_map_raw c
LEFT JOIN batch_type_resolved b ON c.batch_id = b.batch_id;

-- Step 4: Execute Sequenced MERGE Operations (Mapping Tables)
MERGE `{project}.{target_dataset}.map_execution_wave` t
USING current_wave_map s
ON t.execution_id = s.execution_id
WHEN MATCHED THEN UPDATE SET
  t.data_type = s.data_type, t.migration_type = s.migration_type, t.batch_id = s.batch_id, t.batch_name = s.batch_name,
  t.batch_filter = s.batch_filter, t.is_latest_execution = s.is_latest_execution
WHEN NOT MATCHED THEN INSERT(data_type, migration_type, batch_id, batch_name, batch_filter, execution_id, is_latest_execution)
  VALUES(s.data_type, s.migration_type, s.batch_id, s.batch_name, s.batch_filter, s.execution_id, s.is_latest_execution);

UPDATE `{project}.{target_dataset}.map_execution_wave` t
SET is_latest_execution = FALSE
WHERE is_latest_execution = TRUE
  AND EXISTS (
    SELECT 1 FROM current_wave_map s
    WHERE s.batch_id = t.batch_id AND s.is_latest_execution = TRUE AND s.execution_id != t.execution_id
  );

UPDATE `{project}.{target_dataset}.map_execution_wave` t
SET t.batch_name = s.batch_name, t.batch_filter = CONCAT(s.batch_name, " (ID: ", CAST(t.batch_id AS STRING), ")")
FROM latest_wave_names s WHERE t.batch_id = s.wave_id;

-- Relabel all executions of a changed batch, including those outside the lookback window
UPDATE `{project}.{target_dataset}.map_execution_wave` t
SET t.data_type = b.data_type, t.migration_type = COALESCE(t.migration_type, b.migration_type)
FROM batch_type_resolved b
WHERE t.batch_id = b.batch_id AND (t.data_type IS DISTINCT FROM b.data_type OR t.migration_type IS NULL);

-- Relabel snapshots to match the map
UPDATE `{project}.{target_dataset}.snapshot_item_user_wave` s
SET s.data_type = b.data_type
FROM batch_type_resolved b
WHERE s.batch_id = b.batch_id AND s.data_type IS DISTINCT FROM b.data_type;

UPDATE `{project}.{target_dataset}.snapshot_user_wave` s
SET s.data_type = b.data_type
FROM batch_type_resolved b
WHERE s.batch_id = b.batch_id AND s.data_type IS DISTINCT FROM b.data_type;

UPDATE `{project}.{target_dataset}.snapshot_migration_errors` s
SET s.data_type = b.data_type
FROM batch_type_resolved b
WHERE s.batch_id = b.batch_id AND s.data_type IS DISTINCT FROM b.data_type;

-- map_wave_details: one row per batch
MERGE `{project}.{target_dataset}.map_wave_details` t
USING (
  SELECT
    batch_id,
    ANY_VALUE(data_type) AS data_type,
    ARRAY_AGG(batch_name ORDER BY is_latest_execution DESC LIMIT 1)[OFFSET(0)] AS batch_name,
    ARRAY_AGG(batch_filter ORDER BY is_latest_execution DESC LIMIT 1)[OFFSET(0)] AS batch_filter
  FROM `{project}.{target_dataset}.map_execution_wave`
  WHERE batch_id IS NOT NULL
  GROUP BY batch_id
) s
ON t.batch_id = s.batch_id
WHEN MATCHED THEN UPDATE SET t.data_type = s.data_type, t.batch_name = s.batch_name, t.batch_filter = s.batch_filter
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, batch_name, batch_filter) VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter);


-- Step 5: Active Entity Tracking for Incremental Filtering
-- Relabeled batches are always rebuilt
CREATE OR REPLACE TEMP TABLE active_batches AS
SELECT DISTINCT m.batch_id FROM incremental_logs a JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
UNION DISTINCT SELECT DISTINCT wave_id AS batch_id FROM latest_wave_names
UNION DISTINCT SELECT DISTINCT batch_id FROM batch_type_changes;

CREATE OR REPLACE TEMP TABLE active_datatypes AS
SELECT DISTINCT m.data_type FROM incremental_logs a JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
UNION DISTINCT SELECT DISTINCT t.data_type FROM latest_wave_names s JOIN `{project}.{target_dataset}.map_execution_wave` t ON s.wave_id = t.batch_id
UNION DISTINCT SELECT DISTINCT old_data_type FROM batch_type_changes WHERE old_data_type IS NOT NULL
UNION DISTINCT SELECT DISTINCT new_data_type FROM batch_type_changes;


-- Step 6: Update Persistent Snapshots
CREATE OR REPLACE TEMP TABLE incremental_item_user_wave AS
WITH BaseItems AS (
  SELECT
    m.data_type, m.batch_id, COALESCE(a.data_migration.source_name, '(unknown)') AS user_identifier, a.data_migration.source_identifier,
    REGEXP_REPLACE(a.data_migration.source_identifier, r'/versions/[^/]+$', '') AS item_key,
    CASE
      WHEN a.data_migration.source_type LIKE '%File Version' THEN 'version'
      WHEN a.data_migration.source_type LIKE '%File' THEN 'file'
      WHEN a.data_migration.source_type LIKE '%Folder' THEN 'folder'
      ELSE 'other'
    END AS item_class,
    a.event_name, a.data_migration.source_type AS source_type, a.status.event_status, a.time_usec, a.data_migration.execution_id,
    COALESCE(NULLIF(a.data_migration.source_identifier, ''), TO_HEX(MD5(CONCAT(CAST(a.time_usec AS STRING), IFNULL(a.event_name, ''))))) AS item_uuid
  FROM incremental_logs a
  JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
  WHERE LOWER(a.event_type) = 'migration'
    AND (
      a.data_migration.source_type IN (
        'Exchange Online Calendar Event', 'Exchange Online Email Message', 'Exchange Online Contact',
        'Sharepoint File', 'Sharepoint File Version', 'Sharepoint Item', 'OneDrive File', 'OneDrive File Version', 'OneDrive Item',
        'Sharepoint Folder', 'OneDrive Folder', 'Sharepoint Document Library', 'Sharepoint Site', 'Sharepoint Item Crawler'
      )
      OR a.status.event_status = 'FAILED'
    )
),
RankedItems AS (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY batch_id, user_identifier, item_uuid
      ORDER BY CASE WHEN LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END ASC, time_usec DESC
    ) AS rn
  FROM BaseItems
)
SELECT * EXCEPT(rn) FROM RankedItems WHERE rn = 1;

MERGE `{project}.{target_dataset}.snapshot_item_user_wave` t
USING incremental_item_user_wave s
ON t.batch_id = s.batch_id AND t.user_identifier = s.user_identifier AND t.item_uuid = s.item_uuid
WHEN MATCHED AND (
  CASE WHEN LOWER(s.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END < CASE WHEN LOWER(t.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END
  OR (CASE WHEN LOWER(s.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END = CASE WHEN LOWER(t.event_status) IN ('succeeded', 'succeeded_with_warnings') THEN 1 ELSE 2 END AND s.time_usec > t.time_usec)
) THEN UPDATE SET t.event_name = s.event_name, t.source_type = s.source_type, t.event_status = s.event_status, t.time_usec = s.time_usec, t.execution_id = s.execution_id, t.item_key = s.item_key, t.item_class = s.item_class
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, user_identifier, source_identifier, item_key, item_class, item_uuid, event_name, source_type, event_status, time_usec, execution_id)
VALUES(s.data_type, s.batch_id, s.user_identifier, s.source_identifier, s.item_key, s.item_class, s.item_uuid, s.event_name, s.source_type, s.event_status, s.time_usec, s.execution_id);

MERGE `{project}.{target_dataset}.snapshot_user_wave` t
USING (
  WITH UserBaseStats AS (
    SELECT
      m.data_type, m.batch_id, COALESCE(a.data_migration.source_name, '(unknown)') AS user_identifier, MIN(a.time_usec) AS start_time_usec,
      ARRAY_AGG(a.data_migration.execution_id ORDER BY a.time_usec DESC)[OFFSET(0)] AS latest_execution_id, MAX(a.time_usec) AS latest_execution_time_usec
    FROM incremental_logs a
    JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
    WHERE (a.data_migration.source_name IS NOT NULL OR LOWER(a.event_type) = 'migration')
    GROUP BY m.data_type, m.batch_id, user_identifier
  ),
  UserExecCompletions AS (
    SELECT DISTINCT m.batch_id, COALESCE(a.data_migration.source_name, '(unknown)') AS user_identifier, a.data_migration.execution_id, TRUE AS is_completed
    FROM incremental_logs a
    JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
    WHERE a.event_name = 'USER_MIGRATION_COMPLETE'
  )
  SELECT b.data_type, b.batch_id, b.user_identifier, b.start_time_usec, IFNULL(c.is_completed, FALSE) AS is_completed, b.latest_execution_id, b.latest_execution_time_usec
  FROM UserBaseStats b LEFT JOIN UserExecCompletions c ON b.batch_id = c.batch_id AND b.user_identifier = c.user_identifier AND b.latest_execution_id = c.execution_id
) s
ON t.batch_id = s.batch_id AND t.user_identifier = s.user_identifier
WHEN MATCHED THEN UPDATE SET
  t.start_time_usec = LEAST(t.start_time_usec, s.start_time_usec),
  t.is_completed = IF(s.latest_execution_id != IFNULL(t.latest_execution_id, ''), s.is_completed, t.is_completed OR s.is_completed),
  t.latest_execution_id = IF(s.latest_execution_time_usec > IFNULL(t.latest_execution_time_usec, 0), s.latest_execution_id, t.latest_execution_id),
  t.latest_execution_time_usec = GREATEST(IFNULL(t.latest_execution_time_usec, 0), s.latest_execution_time_usec)
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, user_identifier, start_time_usec, is_completed, latest_execution_id, latest_execution_time_usec)
VALUES(s.data_type, s.batch_id, s.user_identifier, s.start_time_usec, s.is_completed, s.latest_execution_id, s.latest_execution_time_usec);

MERGE `{project}.{target_dataset}.snapshot_migration_errors` t
USING (
  SELECT
    m.data_type, m.batch_id, m.batch_name, m.batch_filter, a.data_migration.migration_error_code, a.data_migration.migration_error_title,
    a.status.error_message, a.data_migration.source_name AS user_identifier, a.data_migration.execution_id, a.time_usec,
    TO_HEX(MD5(CONCAT(CAST(a.time_usec AS STRING), "|", IFNULL(a.data_migration.migration_error_code, ""), "|", IFNULL(a.data_migration.source_name, "")))) AS event_uuid
  FROM incremental_logs a
  JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
  WHERE a.status.event_status = 'FAILED' AND a.data_migration.migration_error_code IS NOT NULL AND a.data_migration.migration_error_code != ''
  QUALIFY ROW_NUMBER() OVER(PARTITION BY m.batch_id, TO_HEX(MD5(CONCAT(CAST(a.time_usec AS STRING), "|", IFNULL(a.data_migration.migration_error_code, ""), "|", IFNULL(a.data_migration.source_name, "")))) ORDER BY a.time_usec DESC) = 1
) s
ON t.batch_id = s.batch_id AND t.event_uuid = s.event_uuid
WHEN MATCHED THEN UPDATE SET t.batch_name = s.batch_name, t.batch_filter = s.batch_filter
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, batch_name, batch_filter, migration_error_code, migration_error_title, error_message, user_identifier, execution_id, time_usec, event_uuid)
VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.migration_error_code, s.migration_error_title, s.error_message, s.user_identifier, s.execution_id, s.time_usec, s.event_uuid);


-- Step 6b: Effective User Completion
-- Only Exchange emits USER_MIGRATION_COMPLETE. SharePoint / OneDrive users are treated as complete after 24 hours of inactivity.
CREATE OR REPLACE TEMP TABLE user_completion AS
SELECT
  batch_id, user_identifier, data_type,
  is_completed AS has_completion_event,
  is_completed
    OR (latest_execution_time_usec IS NOT NULL
        AND TIMESTAMP_MICROS(latest_execution_time_usec) <= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR))
    AS is_completed_effective
FROM `{project}.{target_dataset}.snapshot_user_wave`;

-- Step 7: Update Fact Tables
MERGE `{project}.{target_dataset}.fact_datatype_metrics` t
USING (
  WITH UserCounts AS (
    SELECT data_type, COUNT(DISTINCT IF(user_identifier = '(unknown)', NULL, user_identifier)) AS total_users_migrated
    FROM `{project}.{target_dataset}.snapshot_user_wave`
    WHERE data_type IN (SELECT data_type FROM active_datatypes) GROUP BY data_type
  ),
  ItemDedup AS (
    SELECT * FROM `{project}.{target_dataset}.snapshot_item_user_wave`
    WHERE data_type IN (SELECT data_type FROM active_datatypes)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data_type, user_identifier, COALESCE(NULLIF(item_key, ''), item_uuid) ORDER BY (event_name = 'CREATE_FILE') DESC, time_usec DESC) = 1
  ),
  VersionCounts AS (
    SELECT data_type, COUNTIF(item_class = 'version' AND LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS migrated_versions_count
    FROM `{project}.{target_dataset}.snapshot_item_user_wave`
    WHERE data_type IN (SELECT data_type FROM active_datatypes) GROUP BY data_type
  ),
  ItemMetrics AS (
    SELECT data_type,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS successfully_migrated_items,
      COUNTIF(event_status = 'FAILED' AND event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(LOWER(event_status) = 'skipped') AS skipped_items,
      COUNTIF(LOWER(event_status) = 'succeeded_with_warnings') AS warning_items
    FROM ItemDedup GROUP BY data_type
  )
  SELECT u.data_type, u.total_users_migrated, IFNULL(i.successfully_migrated_items, 0) AS successfully_migrated_items, IFNULL(i.failed_items, 0) AS failed_items,
         IFNULL(i.skipped_items, 0) AS skipped_items, IFNULL(i.warning_items, 0) AS warning_items,
         IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) AS total_discovered_items,
         IFNULL(v.migrated_versions_count, 0) AS migrated_versions_count
  FROM UserCounts u LEFT JOIN ItemMetrics i ON u.data_type = i.data_type LEFT JOIN VersionCounts v ON u.data_type = v.data_type
) s
ON t.data_type = s.data_type
WHEN MATCHED THEN UPDATE SET
  t.total_users_migrated = s.total_users_migrated, t.successfully_migrated_items = s.successfully_migrated_items, t.failed_items = s.failed_items,
  t.skipped_items = s.skipped_items, t.warning_items = s.warning_items, t.total_discovered_items = s.total_discovered_items, t.migrated_versions_count = s.migrated_versions_count,
  t.success_percentage = IFNULL(SAFE_DIVIDE(s.successfully_migrated_items, s.successfully_migrated_items + s.failed_items), 0)
WHEN NOT MATCHED THEN INSERT(data_type, total_users_migrated, successfully_migrated_items, failed_items, skipped_items, warning_items, total_discovered_items, migrated_versions_count, success_percentage)
  VALUES(s.data_type, s.total_users_migrated, s.successfully_migrated_items, s.failed_items, s.skipped_items, s.warning_items, s.total_discovered_items, s.migrated_versions_count, IFNULL(SAFE_DIVIDE(s.successfully_migrated_items, s.successfully_migrated_items + s.failed_items), 0));

MERGE `{project}.{target_dataset}.fact_wave_metrics` t
USING (
  WITH WaveUserStats AS (
    -- user_count excludes '(unknown)' users; all_* include them and are only used for status
    SELECT u.batch_id, MIN(TIMESTAMP_MICROS(u.start_time_usec)) AS start_date,
      COUNT(DISTINCT IF(u.user_identifier = '(unknown)', NULL, u.user_identifier)) AS user_count,
      COUNTIF(c.is_completed_effective AND u.user_identifier != '(unknown)') AS completed_user_count,
      COUNT(DISTINCT u.user_identifier) AS all_user_count,
      COUNTIF(c.is_completed_effective) AS all_completed_count,
      -- FALSE if any completion relied on the 24 hour inactivity fallback
      LOGICAL_AND(c.has_completion_event OR NOT c.is_completed_effective) AS all_completions_via_real_event
    FROM `{project}.{target_dataset}.snapshot_user_wave` u JOIN user_completion c ON u.batch_id = c.batch_id AND u.user_identifier = c.user_identifier
    WHERE u.batch_id IN (SELECT batch_id FROM active_batches) GROUP BY u.batch_id
  ),
  WaveBase AS (
    SELECT DISTINCT data_type, batch_id, batch_name, batch_filter FROM `{project}.{target_dataset}.map_execution_wave` WHERE batch_id IN (SELECT batch_id FROM active_batches)
  ),
  ItemDedup AS (
    SELECT * FROM `{project}.{target_dataset}.snapshot_item_user_wave`
    WHERE batch_id IN (SELECT batch_id FROM active_batches)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY batch_id, user_identifier, COALESCE(NULLIF(item_key, ''), item_uuid) ORDER BY (event_name = 'CREATE_FILE') DESC, time_usec DESC) = 1
  ),
  VersionCounts AS (
    SELECT batch_id, COUNTIF(item_class = 'version' AND LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS migrated_versions_count
    FROM `{project}.{target_dataset}.snapshot_item_user_wave` WHERE batch_id IN (SELECT batch_id FROM active_batches) GROUP BY batch_id
  ),
  ItemAggs AS (
    SELECT
      i.batch_id,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')) AS successfully_migrated_items,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type = 'Exchange Online Email Message') AS migrated_mails_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type = 'Exchange Online Calendar Event') AS migrated_calendars_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type = 'Exchange Online Contact') AS migrated_contacts_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type IN ('Sharepoint File', 'OneDrive File', 'Sharepoint File Version', 'OneDrive File Version')) AS migrated_files_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.item_class = 'folder') AS migrated_folders_count,
      COUNTIF(i.event_status = 'FAILED' AND i.event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(LOWER(i.event_status) = 'skipped') AS skipped_items,
      COUNTIF(LOWER(i.event_status) = 'succeeded_with_warnings') AS warning_items,
      COUNTIF(i.event_name = 'CRAWL_FAILURE' AND (i.execution_id = u.latest_execution_id OR u.latest_execution_id IS NULL)) AS crawl_failure_items
    FROM ItemDedup i JOIN `{project}.{target_dataset}.snapshot_user_wave` u ON i.batch_id = u.batch_id AND i.user_identifier = u.user_identifier GROUP BY i.batch_id
  )
  SELECT
    w.data_type, w.batch_id, w.batch_name, w.batch_filter, u.start_date, IFNULL(u.user_count, 0) AS user_count, IFNULL(u.completed_user_count, 0) AS completed_user_count,
    IFNULL(i.successfully_migrated_items, 0) AS successfully_migrated_items, IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0) AS total_migrated_items,
    IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) AS total_discovered_items,
    IFNULL(i.migrated_mails_count, 0) AS migrated_mails_count, IFNULL(i.migrated_calendars_count, 0) AS migrated_calendars_count, IFNULL(i.migrated_contacts_count, 0) AS migrated_contacts_count,
    IFNULL(i.migrated_files_count, 0) AS migrated_files_count, IFNULL(i.migrated_folders_count, 0) AS migrated_folders_count,
    IFNULL(i.skipped_items, 0) AS skipped_items, IFNULL(i.warning_items, 0) AS warning_items, IFNULL(v.migrated_versions_count, 0) AS migrated_versions_count,
    IFNULL(SAFE_DIVIDE(i.successfully_migrated_items, i.successfully_migrated_items + IFNULL(i.failed_items, 0)), 0) AS success_percentage,
    -- NULL instead of 0 when the batch has no named users
    IF(IFNULL(u.user_count, 0) = 0, NULL, IFNULL(SAFE_DIVIDE(u.completed_user_count, u.user_count), 0)) AS completion_percentage,
    IFNULL(SAFE_DIVIDE(IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0), u.user_count), 0) AS avg_items_per_user,
    CASE
      WHEN u.all_user_count = IFNULL(u.all_completed_count, 0) AND u.all_user_count > 0 AND u.all_completions_via_real_event THEN 'Completed'
      WHEN u.all_user_count = IFNULL(u.all_completed_count, 0) AND u.all_user_count > 0 THEN 'Inactive for 24 hours'
      ELSE 'Running'
    END AS status,
    CASE
      WHEN IFNULL(i.successfully_migrated_items, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) = 0 THEN 'No items'
      WHEN IFNULL(i.failed_items, 0) > 0 THEN 'With failures'
      WHEN IFNULL(i.warning_items, 0) > 0 THEN 'With warnings'
      ELSE 'Clean' END AS outcome
  FROM WaveBase w LEFT JOIN WaveUserStats u ON w.batch_id = u.batch_id LEFT JOIN ItemAggs i ON w.batch_id = i.batch_id LEFT JOIN VersionCounts v ON w.batch_id = v.batch_id
) s
ON t.data_type = s.data_type AND t.batch_id = s.batch_id WHEN MATCHED THEN UPDATE SET
  t.batch_name = s.batch_name, t.batch_filter = s.batch_filter, t.start_date = s.start_date, t.user_count = s.user_count, t.completed_user_count = s.completed_user_count,
  t.successfully_migrated_items = s.successfully_migrated_items, t.total_migrated_items = s.total_migrated_items, t.total_discovered_items = s.total_discovered_items, t.migrated_mails_count = s.migrated_mails_count,
  t.migrated_calendars_count = s.migrated_calendars_count, t.migrated_contacts_count = s.migrated_contacts_count, t.migrated_files_count = s.migrated_files_count,
  t.migrated_folders_count = s.migrated_folders_count, t.skipped_items = s.skipped_items, t.warning_items = s.warning_items, t.migrated_versions_count = s.migrated_versions_count,
  t.success_percentage = s.success_percentage, t.completion_percentage = s.completion_percentage, t.avg_items_per_user = s.avg_items_per_user, t.status = s.status, t.outcome = s.outcome
WHEN NOT MATCHED THEN INSERT(
  data_type, batch_id, batch_name, batch_filter, start_date, user_count, completed_user_count, successfully_migrated_items, total_migrated_items, total_discovered_items,
  migrated_mails_count, migrated_calendars_count, migrated_contacts_count, migrated_files_count, migrated_folders_count, skipped_items, warning_items, migrated_versions_count, success_percentage, completion_percentage, avg_items_per_user, status, outcome)
  VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.start_date, s.user_count, s.completed_user_count, s.successfully_migrated_items, s.total_migrated_items, s.total_discovered_items,
  s.migrated_mails_count, s.migrated_calendars_count, s.migrated_contacts_count, s.migrated_files_count, s.migrated_folders_count, s.skipped_items, s.warning_items, s.migrated_versions_count, s.success_percentage, s.completion_percentage, s.avg_items_per_user, s.status, s.outcome);

MERGE `{project}.{target_dataset}.fact_user_wave_metrics` t
USING (
  WITH UserBase AS (
    SELECT u.data_type, u.batch_id, u.user_identifier, c.is_completed_effective AS is_completed, c.has_completion_event, ANY_VALUE(m.batch_name) AS batch_name, ANY_VALUE(m.batch_filter) AS batch_filter
    FROM `{project}.{target_dataset}.snapshot_user_wave` u
    JOIN user_completion c ON u.batch_id = c.batch_id AND u.user_identifier = c.user_identifier
    LEFT JOIN `{project}.{target_dataset}.map_execution_wave` m ON u.batch_id = m.batch_id
    WHERE u.batch_id IN (SELECT batch_id FROM active_batches) GROUP BY u.data_type, u.batch_id, u.user_identifier, c.is_completed_effective, c.has_completion_event
  ),
  ItemDedup AS (
    SELECT * FROM `{project}.{target_dataset}.snapshot_item_user_wave` WHERE batch_id IN (SELECT batch_id FROM active_batches)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY batch_id, user_identifier, COALESCE(NULLIF(item_key, ''), item_uuid) ORDER BY (event_name = 'CREATE_FILE') DESC, time_usec DESC) = 1
  ),
  VersionCounts AS (
    SELECT batch_id, user_identifier, COUNTIF(item_class = 'version' AND LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS migrated_versions_count
    FROM `{project}.{target_dataset}.snapshot_item_user_wave` WHERE batch_id IN (SELECT batch_id FROM active_batches) GROUP BY batch_id, user_identifier
  ),
  ItemCounts AS (
    SELECT i.batch_id, i.user_identifier,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')) AS total_items_migrated,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type = 'Exchange Online Email Message') AS migrated_mails_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type = 'Exchange Online Calendar Event') AS migrated_calendars_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type = 'Exchange Online Contact') AS migrated_contacts_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type IN ('Sharepoint File', 'OneDrive File', 'Sharepoint File Version', 'OneDrive File Version')) AS migrated_files_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.item_class = 'folder') AS migrated_folders_count,
      COUNTIF(i.event_status = 'FAILED' AND i.event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(LOWER(i.event_status) = 'skipped') AS skipped_items,
      COUNTIF(LOWER(i.event_status) = 'succeeded_with_warnings') AS warning_items,
      COUNTIF(i.event_name = 'CRAWL_FAILURE' AND (i.execution_id = u.latest_execution_id OR u.latest_execution_id IS NULL)) AS crawl_failure_items
    FROM ItemDedup i JOIN `{project}.{target_dataset}.snapshot_user_wave` u ON i.batch_id = u.batch_id AND i.user_identifier = u.user_identifier GROUP BY i.batch_id, i.user_identifier
  )
  SELECT
    b.data_type, b.batch_id, b.batch_name, b.batch_filter, b.user_identifier, IFNULL(i.total_items_migrated, 0) AS total_items_migrated,
    IFNULL(i.migrated_mails_count, 0) AS migrated_mails_count, IFNULL(i.migrated_calendars_count, 0) AS migrated_calendars_count, IFNULL(i.migrated_contacts_count, 0) AS migrated_contacts_count,
    IFNULL(i.migrated_files_count, 0) AS migrated_files_count, IFNULL(i.migrated_folders_count, 0) AS migrated_folders_count, IFNULL(i.failed_items, 0) AS failed_items,
    IFNULL(i.skipped_items, 0) AS skipped_items, IFNULL(i.warning_items, 0) AS warning_items, IFNULL(v.migrated_versions_count, 0) AS migrated_versions_count,
    IFNULL(i.crawl_failure_items, 0) AS crawl_failure_items, IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) AS total_items,
    IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) AS total_discovered_items,
    IFNULL(SAFE_DIVIDE(i.total_items_migrated, i.total_items_migrated + IFNULL(i.failed_items, 0)), 0) AS success_rate_percentage,
    CASE
      WHEN NOT b.is_completed THEN 'Running'
      WHEN b.has_completion_event THEN 'Completed'
      ELSE 'Inactive for 24 hours'
    END AS status,
    CASE
      WHEN IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) = 0
           AND IFNULL(i.crawl_failure_items, 0) = 0 THEN 'No items'
      WHEN IFNULL(i.failed_items, 0) > 0 OR IFNULL(i.crawl_failure_items, 0) > 0 THEN 'With failures'
      WHEN IFNULL(i.warning_items, 0) > 0 THEN 'With warnings'
      ELSE 'Clean' END AS outcome
  FROM UserBase b LEFT JOIN ItemCounts i ON b.batch_id = i.batch_id AND b.user_identifier = i.user_identifier LEFT JOIN VersionCounts v ON b.batch_id = v.batch_id AND b.user_identifier = v.user_identifier
) s
ON t.data_type = s.data_type AND t.batch_id = s.batch_id AND t.user_identifier = s.user_identifier WHEN MATCHED THEN UPDATE SET
  t.batch_name = s.batch_name, t.batch_filter = s.batch_filter, t.total_items_migrated = s.total_items_migrated, t.migrated_mails_count = s.migrated_mails_count,
  t.migrated_calendars_count = s.migrated_calendars_count, t.migrated_contacts_count = s.migrated_contacts_count, t.migrated_files_count = s.migrated_files_count,
  t.migrated_folders_count = s.migrated_folders_count, t.failed_items = s.failed_items, t.skipped_items = s.skipped_items, t.warning_items = s.warning_items,
  t.migrated_versions_count = s.migrated_versions_count, t.crawl_failure_items = s.crawl_failure_items, t.total_items = s.total_items, t.total_discovered_items = s.total_discovered_items,
  t.success_rate_percentage = s.success_rate_percentage, t.status = s.status, t.outcome = s.outcome
WHEN NOT MATCHED THEN INSERT(
  data_type, batch_id, batch_name, batch_filter, user_identifier, total_items_migrated, migrated_mails_count, migrated_calendars_count, migrated_contacts_count, migrated_files_count, migrated_folders_count, failed_items, skipped_items, warning_items, migrated_versions_count, crawl_failure_items, total_items, total_discovered_items, success_rate_percentage, status, outcome)
  VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.user_identifier, s.total_items_migrated, s.migrated_mails_count, s.migrated_calendars_count, s.migrated_contacts_count, s.migrated_files_count, s.migrated_folders_count, s.failed_items, s.skipped_items, s.warning_items, s.migrated_versions_count, s.crawl_failure_items, s.total_items, s.total_discovered_items, s.success_rate_percentage, s.status, s.outcome);

MERGE `{project}.{target_dataset}.fact_user_overall_metrics` t
USING (
  WITH UserBase AS (
    SELECT u.data_type, u.user_identifier, LOGICAL_AND(c.is_completed_effective) AS is_completed,
      LOGICAL_AND(c.has_completion_event OR NOT c.is_completed_effective) AS all_via_real_event,
      ANY_VALUE(m.batch_filter) AS batch_filter
    FROM `{project}.{target_dataset}.snapshot_user_wave` u
    JOIN user_completion c ON u.batch_id = c.batch_id AND u.user_identifier = c.user_identifier
    LEFT JOIN `{project}.{target_dataset}.map_execution_wave` m ON u.data_type = m.data_type
    WHERE u.data_type IN (SELECT data_type FROM active_datatypes) GROUP BY u.data_type, u.user_identifier
  ),
  ItemDedup AS (
    SELECT * FROM `{project}.{target_dataset}.snapshot_item_user_wave` WHERE data_type IN (SELECT data_type FROM active_datatypes)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY data_type, user_identifier, COALESCE(NULLIF(item_key, ''), item_uuid) ORDER BY (event_name = 'CREATE_FILE') DESC, time_usec DESC) = 1
  ),
  VersionCounts AS (
    SELECT data_type, user_identifier, COUNTIF(item_class = 'version' AND LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS migrated_versions_count
    FROM `{project}.{target_dataset}.snapshot_item_user_wave` WHERE data_type IN (SELECT data_type FROM active_datatypes) GROUP BY data_type, user_identifier
  ),
  ItemCounts AS (
    SELECT data_type, user_identifier,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS total_items_migrated,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') AND source_type = 'Exchange Online Email Message') AS migrated_mails_count,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') AND source_type = 'Exchange Online Calendar Event') AS migrated_calendars_count,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') AND source_type = 'Exchange Online Contact') AS migrated_contacts_count,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') AND source_type IN ('Sharepoint File', 'OneDrive File', 'Sharepoint File Version', 'OneDrive File Version')) AS migrated_files_count,
      COUNTIF(LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings') AND item_class = 'folder') AS migrated_folders_count,
      COUNTIF(event_status = 'FAILED' AND event_name NOT IN ('CRAWL_FAILURE')) AS failed_items,
      COUNTIF(LOWER(event_status) = 'skipped') AS skipped_items,
      COUNTIF(LOWER(event_status) = 'succeeded_with_warnings') AS warning_items,
      COUNTIF(event_name = 'CRAWL_FAILURE') AS crawl_failure_items
    FROM ItemDedup GROUP BY data_type, user_identifier
  )
  SELECT
    b.user_identifier, b.data_type, b.batch_filter, IFNULL(i.total_items_migrated, 0) AS total_items_migrated, IFNULL(i.migrated_mails_count, 0) AS migrated_mails_count,
    IFNULL(i.migrated_calendars_count, 0) AS migrated_calendars_count, IFNULL(i.migrated_contacts_count, 0) AS migrated_contacts_count,
    IFNULL(i.migrated_files_count, 0) AS migrated_files_count, IFNULL(i.migrated_folders_count, 0) AS migrated_folders_count,
    IFNULL(i.failed_items, 0) AS failed_items, IFNULL(i.skipped_items, 0) AS skipped_items, IFNULL(i.warning_items, 0) AS warning_items, IFNULL(v.migrated_versions_count, 0) AS migrated_versions_count,
    IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) AS total_items,
    IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) AS total_discovered_items,
    IFNULL(SAFE_DIVIDE(i.total_items_migrated, i.total_items_migrated + IFNULL(i.failed_items, 0)), 0) AS success_rate_percentage,
    CASE
      WHEN NOT b.is_completed THEN 'Running'
      WHEN b.all_via_real_event THEN 'Completed'
      ELSE 'Inactive for 24 hours'
    END AS status,
    CASE
      WHEN IFNULL(i.total_items_migrated, 0) + IFNULL(i.failed_items, 0) + IFNULL(i.skipped_items, 0) = 0
           AND IFNULL(i.crawl_failure_items, 0) = 0 THEN 'No items'
      WHEN IFNULL(i.failed_items, 0) > 0 OR IFNULL(i.crawl_failure_items, 0) > 0 THEN 'With failures'
      WHEN IFNULL(i.warning_items, 0) > 0 THEN 'With warnings'
      ELSE 'Clean' END AS outcome
  FROM UserBase b LEFT JOIN ItemCounts i ON b.data_type = i.data_type AND b.user_identifier = i.user_identifier LEFT JOIN VersionCounts v ON b.data_type = v.data_type AND b.user_identifier = v.user_identifier
) s
ON t.data_type = s.data_type AND t.user_identifier = s.user_identifier WHEN MATCHED THEN UPDATE SET
  t.batch_filter = s.batch_filter, t.total_items_migrated = s.total_items_migrated, t.migrated_mails_count = s.migrated_mails_count, t.migrated_calendars_count = s.migrated_calendars_count,
  t.migrated_contacts_count = s.migrated_contacts_count, t.migrated_files_count = s.migrated_files_count, t.migrated_folders_count = s.migrated_folders_count,
  t.failed_items = s.failed_items, t.skipped_items = s.skipped_items, t.warning_items = s.warning_items, t.migrated_versions_count = s.migrated_versions_count,
  t.total_items = s.total_items, t.total_discovered_items = s.total_discovered_items, t.success_rate_percentage = s.success_rate_percentage, t.status = s.status, t.outcome = s.outcome
WHEN NOT MATCHED THEN INSERT(
  user_identifier, data_type, batch_filter, total_items_migrated, migrated_mails_count, migrated_calendars_count, migrated_contacts_count, migrated_files_count, migrated_folders_count, failed_items, skipped_items, warning_items, migrated_versions_count, total_items, total_discovered_items, success_rate_percentage, status, outcome)
  VALUES(s.user_identifier, s.data_type, s.batch_filter, s.total_items_migrated, s.migrated_mails_count, s.migrated_calendars_count, s.migrated_contacts_count, s.migrated_files_count, s.migrated_folders_count, s.failed_items, s.skipped_items, s.warning_items, s.migrated_versions_count, s.total_items, s.total_discovered_items, s.success_rate_percentage, s.status, s.outcome);

DELETE FROM `{project}.{target_dataset}.fact_migration_errors` WHERE batch_id IN (SELECT batch_id FROM active_batches);
INSERT INTO `{project}.{target_dataset}.fact_migration_errors` (data_type, batch_id, batch_name, batch_filter, migration_error_code, migration_error_title, error_message, user_identifier, occurrence_count)
SELECT s.data_type, s.batch_id, ANY_VALUE(s.batch_name) AS batch_name, ANY_VALUE(s.batch_filter) AS batch_filter, s.migration_error_code, ANY_VALUE(s.migration_error_title) AS migration_error_title, s.error_message, s.user_identifier, COUNT(*) AS occurrence_count
FROM `{project}.{target_dataset}.snapshot_migration_errors` s JOIN `{project}.{target_dataset}.map_execution_wave` m ON s.batch_id = m.batch_id AND s.execution_id = m.execution_id
WHERE s.batch_id IN (SELECT batch_id FROM active_batches) AND m.is_latest_execution = TRUE GROUP BY s.data_type, s.batch_id, s.migration_error_code, s.error_message, s.user_identifier;

-- Timeline: dedupe per item/day (CREATE_FILE preferred), then keep successful items
MERGE `{project}.{target_dataset}.fact_migration_timeline` t USING (
  WITH DailyUniqueItems AS (
    SELECT m.data_type, m.batch_id, m.batch_name, m.batch_filter, DATE(TIMESTAMP_MICROS(a.time_usec)) AS migration_date,
      LOWER(a.status.event_status) AS st,
      ROW_NUMBER() OVER (PARTITION BY m.batch_id, DATE(TIMESTAMP_MICROS(a.time_usec)), IF(IFNULL(a.data_migration.source_identifier, '') = '', GENERATE_UUID(), REGEXP_REPLACE(a.data_migration.source_identifier, r'/versions/[^/]+$', '')) ORDER BY (a.event_name = 'CREATE_FILE') DESC, IF(LOWER(a.status.event_status) IN ('succeeded', 'succeeded_with_warnings'), 0, 1), a.time_usec DESC) AS rn
    FROM incremental_logs a JOIN `{project}.{target_dataset}.map_execution_wave` m ON a.data_migration.execution_id = m.execution_id
    WHERE LOWER(a.event_type) = 'migration'
      AND a.data_migration.source_type IN ('Exchange Online Calendar Event', 'Exchange Online Email Message', 'Exchange Online Contact', 'Sharepoint File', 'OneDrive File', 'Sharepoint File Version', 'OneDrive File Version', 'Sharepoint Item', 'OneDrive Item', 'Sharepoint Folder', 'OneDrive Folder', 'Sharepoint Document Library', 'Sharepoint Site', 'Sharepoint Item Crawler')
      AND DATE(TIMESTAMP_MICROS(a.time_usec)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  )
  SELECT data_type, batch_id, batch_name, batch_filter, migration_date, COUNT(*) AS items_migrated
  FROM DailyUniqueItems
  WHERE rn = 1 AND st IN ('succeeded', 'succeeded_with_warnings')
  GROUP BY data_type, batch_id, batch_name, batch_filter, migration_date
) s ON t.data_type = s.data_type AND t.batch_id = s.batch_id AND t.migration_date = s.migration_date
WHEN MATCHED THEN UPDATE SET t.batch_name = s.batch_name, t.batch_filter = s.batch_filter, t.items_migrated = s.items_migrated
WHEN NOT MATCHED THEN INSERT(data_type, batch_id, batch_name, batch_filter, migration_date, items_migrated) VALUES(s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.migration_date, s.items_migrated);

-- One row per (data_type, error title); the dashboard applies the top 10 limit
DELETE FROM `{project}.{target_dataset}.fact_top_errors` WHERE TRUE;
INSERT INTO `{project}.{target_dataset}.fact_top_errors` (data_type, migration_error_title, error_message, user_identifier, occurrence_count)
WITH RankedUniqueErrors AS (
  SELECT data_type, migration_error_title, error_message, user_identifier,
    SUM(occurrence_count) OVER (PARTITION BY data_type, migration_error_title) AS total_occurrence_count,
    ROW_NUMBER() OVER (PARTITION BY data_type, migration_error_title ORDER BY occurrence_count DESC) AS rn
  FROM `{project}.{target_dataset}.fact_migration_errors`
)
SELECT data_type, migration_error_title, error_message, user_identifier, total_occurrence_count AS occurrence_count
FROM RankedUniqueErrors WHERE rn = 1;


-- Step 8: Per-Run Execution Metrics
MERGE `{project}.{target_dataset}.fact_execution_metrics` t
USING (
  WITH Executions AS (
    SELECT execution_id, batch_id, batch_name, batch_filter, data_type, is_latest_execution
    FROM `{project}.{target_dataset}.map_execution_wave`
  ),
  RawItems AS (
    SELECT
      e.batch_id, e.execution_id, e.batch_name, e.batch_filter, e.data_type, e.is_latest_execution,
      COALESCE(a.data_migration.source_name, '(unknown)') AS user_identifier,
      REGEXP_REPLACE(a.data_migration.source_identifier, r'/versions/[^/]+$', '') AS item_key,
      CASE
        WHEN a.data_migration.source_type LIKE '%File Version' THEN 'version'
        WHEN a.data_migration.source_type LIKE '%File' THEN 'file'
        WHEN a.data_migration.source_type LIKE '%Folder' THEN 'folder'
        ELSE 'other'
      END AS item_class,
      a.event_name, a.data_migration.source_type AS source_type, a.status.event_status, a.time_usec,
      COALESCE(NULLIF(a.data_migration.source_identifier, ''), TO_HEX(MD5(CONCAT(CAST(a.time_usec AS STRING), IFNULL(a.event_name, ''))))) AS item_uuid
    FROM incremental_logs a
    JOIN Executions e ON a.data_migration.execution_id = e.execution_id
    WHERE LOWER(a.event_type) = 'migration'
      AND (
        a.data_migration.source_type IN (
          'Exchange Online Calendar Event', 'Exchange Online Email Message', 'Exchange Online Contact',
          'Sharepoint File', 'Sharepoint File Version', 'Sharepoint Item', 'OneDrive File', 'OneDrive File Version', 'OneDrive Item',
          'Sharepoint Folder', 'OneDrive Folder', 'Sharepoint Document Library', 'Sharepoint Site', 'Sharepoint Item Crawler'
        )
        OR a.status.event_status = 'FAILED'
      )
  ),
  ExecutionDedup AS (
    SELECT * FROM RawItems
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY execution_id, user_identifier, COALESCE(NULLIF(item_key, ''), item_uuid)
      ORDER BY (event_name = 'CREATE_FILE') DESC, time_usec DESC
    ) = 1
  ),
  ExecutionVersionCounts AS (
    SELECT execution_id,
           COUNTIF(item_class = 'version' AND LOWER(event_status) IN ('succeeded', 'succeeded_with_warnings')) AS migrated_versions_count
    FROM RawItems
    GROUP BY execution_id
  ),
  StartTimes AS (
    SELECT data_migration.execution_id AS execution_id, MIN(time_usec) AS start_usec
    FROM incremental_logs WHERE event_name = 'START_MIGRATION' GROUP BY 1
  ),
  ExecutionAggs AS (
    SELECT
      i.execution_id, i.batch_id, i.batch_name, i.batch_filter, i.data_type, i.is_latest_execution,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')) AS successfully_migrated_items,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.source_type IN ('Sharepoint File', 'Sharepoint File Version', 'OneDrive File', 'OneDrive File Version')) AS migrated_files_count,
      COUNTIF(LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings') AND i.item_class = 'folder') AS migrated_folders_count,
      COUNTIF(LOWER(i.event_status) = 'failed' AND i.event_name != 'CRAWL_FAILURE') AS failed_items,
      COUNTIF(LOWER(i.event_status) = 'skipped') AS skipped_items,
      COUNTIF(LOWER(i.event_status) = 'succeeded_with_warnings') AS warning_items
    FROM ExecutionDedup i
    GROUP BY i.execution_id, i.batch_id, i.batch_name, i.batch_filter, i.data_type, i.is_latest_execution
  )
  SELECT a.*, TIMESTAMP_MICROS(st.start_usec) AS run_start,
         a.successfully_migrated_items + a.failed_items + a.skipped_items AS total_discovered_items,
         IFNULL(v.migrated_versions_count, 0) AS migrated_versions_count,
         IFNULL(SAFE_DIVIDE(a.successfully_migrated_items, a.successfully_migrated_items + a.failed_items), 0) AS success_percentage
  FROM ExecutionAggs a
  LEFT JOIN ExecutionVersionCounts v ON a.execution_id = v.execution_id
  LEFT JOIN StartTimes st ON a.execution_id = st.execution_id
) s
ON t.execution_id = s.execution_id
WHEN MATCHED THEN UPDATE SET
  t.batch_id = s.batch_id, t.batch_name = s.batch_name, t.batch_filter = s.batch_filter, t.data_type = s.data_type,
  t.run_start = s.run_start, t.is_latest_execution = s.is_latest_execution,
  t.successfully_migrated_items = s.successfully_migrated_items,
  t.migrated_files_count = s.migrated_files_count, t.migrated_folders_count = s.migrated_folders_count,
  t.failed_items = s.failed_items, t.skipped_items = s.skipped_items, t.warning_items = s.warning_items,
  t.total_discovered_items = s.total_discovered_items,
  t.migrated_versions_count = s.migrated_versions_count, t.success_percentage = s.success_percentage
WHEN NOT MATCHED THEN INSERT(
  execution_id, run_start, is_latest_execution, batch_id, batch_name, batch_filter, data_type, successfully_migrated_items,
  migrated_files_count, migrated_folders_count, failed_items, skipped_items, warning_items,
  total_discovered_items, migrated_versions_count, success_percentage)
VALUES(
  s.execution_id, s.run_start, s.is_latest_execution, s.batch_id, s.batch_name, s.batch_filter, s.data_type, s.successfully_migrated_items,
  s.migrated_files_count, s.migrated_folders_count, s.failed_items, s.skipped_items, s.warning_items,
  s.total_discovered_items, s.migrated_versions_count, s.success_percentage);

-- Step 9: Sync latest-run flag and data_type for runs outside the lookback window
UPDATE `{project}.{target_dataset}.fact_execution_metrics` f
SET f.is_latest_execution = m.is_latest_execution, f.data_type = m.data_type
FROM `{project}.{target_dataset}.map_execution_wave` m
WHERE f.execution_id = m.execution_id
  AND (f.is_latest_execution IS DISTINCT FROM m.is_latest_execution OR f.data_type IS DISTINCT FROM m.data_type);

-- Step 10: Remove fact rows left under an old data_type label
DELETE FROM `{project}.{target_dataset}.fact_wave_metrics` t
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{target_dataset}.map_execution_wave` m
  WHERE m.batch_id = t.batch_id AND m.data_type = t.data_type);

DELETE FROM `{project}.{target_dataset}.fact_migration_timeline` t
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{target_dataset}.map_execution_wave` m
  WHERE m.batch_id = t.batch_id AND m.data_type = t.data_type);

DELETE FROM `{project}.{target_dataset}.fact_user_wave_metrics` t
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{target_dataset}.snapshot_user_wave` s
  WHERE s.batch_id = t.batch_id AND s.user_identifier = t.user_identifier AND s.data_type = t.data_type);

DELETE FROM `{project}.{target_dataset}.fact_user_overall_metrics` t
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{target_dataset}.snapshot_user_wave` s
  WHERE s.user_identifier = t.user_identifier AND s.data_type = t.data_type);

DELETE FROM `{project}.{target_dataset}.fact_datatype_metrics` t
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{target_dataset}.snapshot_user_wave` s
  WHERE s.data_type = t.data_type);

-- Step 11: Update Failed Item Event History
CREATE OR REPLACE TEMP TABLE incremental_item_failures AS
SELECT
  m.data_type, m.batch_id, m.batch_name, m.batch_filter, a.data_migration.execution_id,
  COALESCE(a.data_migration.source_name, '(unknown)') AS user_identifier,
  a.event_name, a.data_migration.source_type, a.data_migration.source_identifier, a.data_migration.source_uri,
  a.data_migration.migration_error_code, a.data_migration.migration_error_title, a.status.error_message,
  a.time_usec,
  TO_HEX(MD5(CONCAT(a.data_migration.execution_id, "|", CAST(a.time_usec AS STRING), "|",
    IFNULL(a.data_migration.source_identifier, ''), "|", IFNULL(a.event_name, '')))) AS event_uuid
FROM incremental_logs a
JOIN `{project}.{target_dataset}.map_execution_wave` m
  ON a.data_migration.execution_id = m.execution_id
WHERE a.status.event_status = 'FAILED'
-- The source log contains exact duplicate events; keep one per event_uuid so the MERGE does not fail
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_uuid) = 1;

MERGE `{project}.{target_dataset}.snapshot_item_failures` t
USING incremental_item_failures s
ON t.event_uuid = s.event_uuid
WHEN MATCHED THEN UPDATE SET
  t.batch_name = s.batch_name, t.batch_filter = s.batch_filter,
  t.migration_error_code = s.migration_error_code, t.migration_error_title = s.migration_error_title,
  t.error_message = s.error_message
WHEN NOT MATCHED THEN INSERT(
  data_type, batch_id, batch_name, batch_filter, execution_id, user_identifier, event_name,
  source_type, source_identifier, source_uri, migration_error_code, migration_error_title,
  error_message, time_usec, event_uuid)
VALUES(
  s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.execution_id, s.user_identifier, s.event_name,
  s.source_type, s.source_identifier, s.source_uri, s.migration_error_code, s.migration_error_title,
  s.error_message, s.time_usec, s.event_uuid);

-- Step 12: Update Failed Items (one row per batch_id + source_identifier)
CREATE OR REPLACE TEMP TABLE failure_firsts AS
SELECT
  batch_id, source_identifier,
  ANY_VALUE(data_type) AS data_type,
  ANY_VALUE(batch_name) AS batch_name,
  ANY_VALUE(batch_filter) AS batch_filter,
  ANY_VALUE(user_identifier) AS user_identifier,
  ARRAY_AGG(STRUCT(execution_id, time_usec, source_type, source_uri, migration_error_code, migration_error_title, error_message)
    ORDER BY time_usec ASC LIMIT 1)[OFFSET(0)] AS first_failure,
  ARRAY_AGG(STRUCT(execution_id, time_usec, source_type, source_uri, migration_error_code, migration_error_title, error_message)
    ORDER BY time_usec DESC LIMIT 1)[OFFSET(0)] AS last_failure
FROM `{project}.{target_dataset}.snapshot_item_failures`
WHERE batch_id IN (SELECT batch_id FROM active_batches) AND source_identifier != ''
GROUP BY batch_id, source_identifier;

MERGE `{project}.{target_dataset}.fact_failed_items` t
USING (
  WITH CurrentStatus AS (
    SELECT
      f.*,
      i.execution_id AS current_execution_id,
      i.time_usec AS current_time_usec,
      i.event_status AS current_status,
      (i.event_status IS NOT NULL AND LOWER(i.event_status) IN ('succeeded', 'succeeded_with_warnings')) AS is_resolved
    FROM failure_firsts f
    LEFT JOIN `{project}.{target_dataset}.snapshot_item_user_wave` i
      ON i.batch_id = f.batch_id AND i.user_identifier = f.user_identifier AND i.item_uuid = f.source_identifier
  )
  SELECT
    data_type, batch_id, batch_name, batch_filter, user_identifier, source_identifier,
    -- Location fields come from the most recent failure
    last_failure.source_type AS source_type,
    last_failure.source_uri AS source_uri,
    first_failure.execution_id AS initial_execution_id,
    first_failure.time_usec AS initial_time_usec,
    first_failure.migration_error_code AS initial_migration_error_code,
    first_failure.migration_error_title AS initial_migration_error_title,
    first_failure.error_message AS initial_error_message,
    COALESCE(current_execution_id, last_failure.execution_id) AS latest_execution_id,
    COALESCE(current_time_usec, last_failure.time_usec) AS latest_time_usec,
    -- Defaults to FAILED if the item is missing from snapshot_item_user_wave
    COALESCE(current_status, 'FAILED') AS latest_status,
    -- Clear error fields once resolved
    IF(is_resolved, NULL, last_failure.migration_error_code) AS latest_migration_error_code,
    IF(is_resolved, NULL, last_failure.migration_error_title) AS latest_migration_error_title,
    IF(is_resolved, NULL, last_failure.error_message) AS latest_error_message,
    IF(is_resolved, 'Resolved', 'Open') AS resolution_status
  FROM CurrentStatus
) s
ON t.batch_id = s.batch_id AND t.source_identifier = s.source_identifier
WHEN MATCHED THEN UPDATE SET
  t.batch_name = s.batch_name, t.batch_filter = s.batch_filter,
  t.source_type = s.source_type, t.source_uri = s.source_uri,
  t.latest_execution_id = s.latest_execution_id, t.latest_time_usec = s.latest_time_usec,
  t.latest_status = s.latest_status, t.latest_migration_error_code = s.latest_migration_error_code,
  t.latest_migration_error_title = s.latest_migration_error_title, t.latest_error_message = s.latest_error_message,
  t.resolution_status = s.resolution_status
WHEN NOT MATCHED THEN INSERT(
  data_type, batch_id, batch_name, batch_filter, user_identifier, source_type, source_identifier, source_uri,
  initial_execution_id, initial_time_usec, initial_migration_error_code, initial_migration_error_title, initial_error_message,
  latest_execution_id, latest_time_usec, latest_status, latest_migration_error_code, latest_migration_error_title, latest_error_message,
  resolution_status)
VALUES(
  s.data_type, s.batch_id, s.batch_name, s.batch_filter, s.user_identifier, s.source_type, s.source_identifier, s.source_uri,
  s.initial_execution_id, s.initial_time_usec, s.initial_migration_error_code, s.initial_migration_error_title, s.initial_error_message,
  s.latest_execution_id, s.latest_time_usec, s.latest_status, s.latest_migration_error_code, s.latest_migration_error_title, s.latest_error_message,
  s.resolution_status);