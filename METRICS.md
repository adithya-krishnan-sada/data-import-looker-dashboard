# Understanding your report
## Import Metrics Reference

This document provides a comprehensive reference of the metrics defined and computed in the reporting dashboard.

The dashboard reports on three data types: **Exchange Online**, **SharePoint Online**, and **OneDrive**. SharePoint and OneDrive content imported through `SharePoint Online Enterprise Migration` is assigned to one of the two per batch, based on the majority of item source URLs (`-my.sharepoint.com` or `/personal/` indicates OneDrive).

Each item is counted once per user, using its best known status: a later success replaces an earlier failure. Users without a `source_name` in the logs are shown as `(unknown)` and are excluded from user counts.

### Item metrics

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Total Imported Items** | User / Batch / Data type | Number of items with `SUCCEEDED` or `SUCCEEDED_WITH_WARNINGS` status. | Shows the number of successfully imported items, broken down into email messages, calendar events, contacts, files (including file versions), and folders. |
| **Total Failed Items** | User / Batch / Data type | Number of items with `FAILED` status, excluding `CRAWL_FAILURE` events. | Shows the number of items that failed to import and have not succeeded on a later attempt. |
| **Total Processed Items** | User / Batch | `Successful Items + Failed Items` | Shows the total number of items that were imported or failed to import. |
| **Skipped Items** | User / Batch / Data type | Number of items with `SKIPPED` status. | Shows the number of items that were skipped during the import. |
| **Total Discovered Items** | User / Batch / Data type | `Successful Items + Failed Items + Skipped Items` | Shows the total number of items found for the import, whatever their result. |
| **Items with Warnings** | User / Batch / Data type | Number of items with `SUCCEEDED_WITH_WARNINGS` status. | Shows the number of imported items that completed with warnings. These are also included in **Total Imported Items**. |
| **Imported File Versions** | User / Batch / Data type | Number of successful items with a `File Version` source type. | Shows the number of SharePoint and OneDrive file versions that were imported. |
| **Success Percentage** | User / Batch / Data type | `Successful Items / (Successful Items + Failed Items)` | Shows the number of successfully imported items compared to the total number of processed items. |
| **Daily Imported Items** | Batch / Day | Number of unique items with a successful status per day, for the last year. | Shows import throughput over time. An item imported several times on the same day is counted once. |

### User metrics

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Total Imported Users** | Batch / Data type | Number of distinct users, excluding `(unknown)`. | Shows the number of user identifiers, such as `source_name`, that are included in one batch or data type. |
| **Completed Users** | Batch | Number of users whose import is complete. | A user is complete when a `USER_MIGRATION_COMPLETE` event is logged for their latest run. SharePoint and OneDrive do not log this event, so those users are treated as complete after 24 hours without new activity. |
| **User Completion Percentage** | Batch | `Completed Users / Total Imported Users` | Shows the number of completed users compared to the total number of users in the batch. The value is blank when the batch has no named users. |
| **Average Items per User** | Batch | `(Successful Items + Failed Items) / Total Imported Users` | Shows the average number of processed items for each user in a batch. |
| **User Import Status** | User | `Running`, `Completed`, or `Inactive for 24 hours` | Shows the progress of a user's import:<br>• **Running:** The import is still in progress<br>• **Completed:** A completion event was logged<br>• **Inactive for 24 hours:** No completion event, but no new activity for 24 hours (SharePoint and OneDrive) |
| **User Import Outcome** | User | `No items`, `With failures`, `With warnings`, or `Clean` | Shows the result of a user's import:<br>• **No items:** No items or crawl failures were found<br>• **With failures:** At least one failed item or crawl failure<br>• **With warnings:** No failures, but at least one item with warnings<br>• **Clean:** All items imported without warnings |

### Batch metrics

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Batch Status** | Batch | `Completed` when all users are complete and every completion has a completion event. `Inactive for 24 hours` when all users are complete but at least one relies on the 24 hour inactivity rule. Otherwise `Running`. | Shows the progress of the batch. |
| **Batch Outcome** | Batch | `No items`, `With failures`, `With warnings`, or `Clean` | Shows the result of the batch, using the same rules as **User Import Outcome** based on item counts. |

### Error metrics

| Metric Name | Aggregation Level | Technical Calculation Logic | Definition |
| :--- | :--- | :--- | :--- |
| **Error Occurrence** | Batch / User | Total number of unique `migration_error_code` instances in the last data import. | Shows a list of unique `migration_error_code` occurrences for the last data import, including a list of errors according to how often they occur. |
| **Top Errors (Top Blocker Reasons)** | Data type | Top 10 unique `migration_error_title` entries for each data type, ranked by the sum of `occurrence_count` in the latest execution of each batch. | Shows the most common migration errors across all batches based on total occurrences in the latest runs, highlighting the most affected user and error message. |
| **Discovery Failure Count** | Batch / User | Total number of `CRAWL_FAILURE` events in the user's latest run. | Shows the number of items that could not be discovered during each user's data import. |
| **Undiscoverable Users** | Batch | Total number of users with crawler failures. | Shows the number of users that failed to crawl during the last data import. |
| **Failed Items** | Item | One row per item that has failed at least once, identified by batch and `source_identifier`. | Shows the first and latest error for each failed item, along with its latest status. The item is marked **Resolved** once it imports successfully, otherwise **Open**. Failures without a `source_identifier`, such as some crawl failures, are not listed. |
