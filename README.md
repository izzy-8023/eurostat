# eurostat - Healthcare
## 1. Introduction
## 2. Metadata
### 2.1 Table_of_Contents
- When interested in working with from Eurostat navigation tree , a classification of Eurostat datasets into hierarchical categories, it is possible to retrieve a TXT or XML representation named "table of contents" (TOC)

- Please consult API - Detailed guidelines - Catalogue API - TOC for the information provided by each format
- https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/catalogue-api/toc
- Download link

- XML

- https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml
 - XML table of contents is multilingual 

- TXT
- One text file is available per language:
- english : https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en  
- french : https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=fr 
- german : https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=de 

### 2.2 DCAT - Data Catalogue Vocabulary
- The catalogue of datasets can be retrieved as a set of RDF files expressed following Data Catalogue Vocabulary - Application Profile (DCAT-AP).

- This data catalogue is sent by Eurostat to https://data.europa.eu/en portal on a daily basis.

- Download link
- FULL
- Retrieve the full catalogue

- https://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/FULL

- UPDATES
- Updates are the list of modified datasets since last update.

 - Eurostat datasets are updated twice a day, at 11:00 and at 23:00 in Europe/Brussels time zone

- http://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/UPDATES

- ### 2.3 RSS feed
- RSS (Rich Site Summary) is a type of web feed that allows users to access updates to online content in a standardised, computer-readable format.

- It allows to be informed about the last changes carried out to data products and code lists published by Eurostat.

- Here are the URL’s of the RSS feeds:

- English version: https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss 
- German version: https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/de/statistics-update.rss 
- French version: https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/fr/statistics-update.rss 
- For further details, please refer to API - Detailed guidelines - Catalogue API - RSS


### 2.4 Eurostat metabase.txt.gz
The metabase file includes the structure definition of all available Eurostat datasets via the API i.e. the dimensions codes used and their related positions codes in a delimiter-separated format separated by tabulations (TAB). One line of this file as the structure "Dataset Code TAB Dimension Code TAB Position Code" .

This list is alphabetically sorted on dataset code then dimension code and finally on position code list order.

https://ec.europa.eu/eurostat/api/dissemination/catalogue/metabase.txt.gz

Eurostat metabase is refreshed twice a day, at 11:00 and at 23:00 in Europe/Brussels time zone


## 3. Datasets
### Size
- Loading catalog from eurostat_catalog.json...
- Filtering datasets with keyword 'HLTH'...
- Found 490 datasets to process.

- --- Total Size Calculation Summary ---
- Datasets targeted:                 490
- Successfully processed and sized:  490
- Failed or size unavailable:        0
 
- Total calculated size from successful downloads: 6844.20 MB (6.68 GB)

# Notes
## 1. Airflow DAGs

**Core Concepts:**

*   **Persistence:** You need a persistent location accessible by Airflow workers to store:
    *   The downloaded raw JSON datasets (e.g., `Data_Directory` on a shared volume, or preferably, a cloud storage bucket like S3/GCS). Let's call this `RAW_DATA_PATH`.
    *   The processed/parsed output files (e.g., `Output_Directory` or another location in cloud storage). Let's call this `PROCESSED_DATA_PATH`.
    *   The filtered list of "HLTH" dataset IDs generated monthly (e.g., a simple text file or JSON file in a persistent location like S3/GCS, or even an Airflow Variable if the list isn't huge). Let's call this `HEALTH_ID_LIST_PATH`.
*   **Idempotency:** Tasks should ideally be runnable multiple times without causing issues (e.g., downloading and overwriting a file is idempotent).
*   **Modularity:** Refactor your Python scripts (`SourceData.py`, `jsonParser.py`) so their core logic (filtering, downloading, parsing) is in importable functions.

**DAG 1: `eurostat_initial_catalog_and_bulk_load`**

*   **Schedule:** `@monthly` (or `None` for manual trigger after initial setup)
*   **Purpose:** Handles steps 1, 2, and 3 (initial catalog processing, bulk download, bulk parse).
*   **Tasks:**
    1.  **`download_catalog_task`**:
        *   Operator: `BashOperator` (using `curl`) or `PythonOperator` (using `requests`).
        *   Action: Downloads `eurostat_catalog.json` to a temporary location within the Airflow task's context or directly to a staging area in your persistent storage.
        *   Output: Path to the downloaded catalog file (via XCom if needed by the next task).
    2.  **`filter_and_save_health_ids_task`**:
        *   Operator: `PythonOperator`.
        *   Action:
            *   Takes the path to the catalog file (from XCom or known location).
            *   Calls a refactored `health_dataset_list` function (imported from your refactored `SourceData.py` module).
            *   Saves the resulting list of `(id, label)` tuples or just IDs to the persistent `HEALTH_ID_LIST_PATH` (e.g., as `health_ids.txt` or `health_ids.json` on S3/GCS). Overwrites if it exists.
        *   Dependency: `download_catalog_task`.
    3.  **`initial_bulk_download_task`**:
        *   Operator: `PythonOperator`.
        *   Action:
            *   Reads the dataset IDs from `HEALTH_ID_LIST_PATH`.
            *   Calls a refactored `download_datasets_concurrently` function (based on the multi-threaded part of your `SourceData.py`, modified to **save permanently** to `RAW_DATA_PATH`, not a temp dir, and **not delete**). This function takes the list of IDs and the target directory.
            *   Logs successes and failures. Critically fails the task if a significant number of downloads fail initially.
        *   Dependency: `filter_and_save_health_ids_task`.
    4.  **`initial_bulk_parse_task`**:
        *   Operator: `PythonOperator`.
        *   Action:
            *   Reads the dataset IDs from `HEALTH_ID_LIST_PATH`.
            *   Calls a refactored `parse_datasets_concurrently` function (based on your `jsonParser.py`, likely using `concurrent.futures` `ThreadPoolExecutor` or `ProcessPoolExecutor` as parsing can be CPU/IO intensive).
            *   This function iterates through the IDs, constructs input paths from `RAW_DATA_PATH`, constructs output paths to `PROCESSED_DATA_PATH` (e.g., Parquet or CSV), and calls the single-file parsing logic for each.
            *   Logs successes and failures.
        *   Dependency: `initial_bulk_download_task`.

**DAG 2: `eurostat_daily_update`**

*   **Schedule:** `0 6,18 * * *` (or your desired twice-daily schedule, e.g., 6:00 AM and 6:00 PM UTC)
*   **Purpose:** Handles step 4 (checking RSS, downloading updates, replacing, parsing updates).
*   **Tasks:**
    1.  **`get_updated_ids_from_rss_task`**:
        *   Operator: `PythonOperator`.
        *   Action:
            *   Needs a new Python function using a library like `feedparser` (`pip install feedparser`).
            *   Fetches the Eurostat RSS feed URL (you'll need to find the correct one).
            *   Parses the feed entries.
            *   **Crucially:** Filters entries to identify *which specific dataset IDs* have been updated (this might require careful inspection of feed entry titles or content). You might need to cross-reference with the full list loaded from `HEALTH_ID_LIST_PATH` to only consider health datasets.
            *   Pushes the list of *updated* dataset IDs via XCom. If no updates are found, push an empty list.
    2.  **`trigger_update_processing` (Optional Branching):**
        *   Operator: `BranchPythonOperator`.
        *   Action: Checks if the list of updated IDs from XCom is empty.
        *   If empty, branches to a `no_updates_task` (DummyOperator).
        *   If not empty, branches to `download_updated_task`.
    3.  **`no_updates_task`**:
        *   Operator: `DummyOperator`.
        *   Action: Placeholder indicating no updates were needed for this run.
    4.  **`download_updated_task`**:
        *   Operator: `PythonOperator`.
        *   Action:
            *   Pulls the list of *updated* dataset IDs from XCom.
            *   Calls the *same* refactored `download_datasets_concurrently` function as in DAG 1, but passes only the *updated* IDs. This function should **overwrite** existing files in `RAW_DATA_PATH`.
            *   Pushes the list of successfully downloaded/updated IDs via XCom.
        *   Dependency: Triggered by the branch operator.
    5.  **`parse_updated_task`**:
        *   Operator: `PythonOperator`.
        *   Action:
            *   Pulls the list of *successfully updated* dataset IDs from XCom (output of `download_updated_task`).
            *   Calls the *same* refactored `parse_datasets_concurrently` function as in DAG 1, but passes only the *updated* IDs.
            *   This function parses the updated raw files and overwrites the corresponding processed files in `PROCESSED_DATA_PATH`.
        *   Dependency: `download_updated_task`.

**Implementation Details & Considerations:**

*   **Refactoring:** You MUST refactor your existing scripts into modules with functions that can be imported and called by the `PythonOperator`. Avoid large blocks of logic directly in the operator's `python_callable`.
*   **Python Virtual Environment:** Use Airflow's `PythonVirtualenvOperator` or manage dependencies carefully if using `PythonOperator` directly, ensuring `requests`, `ijson`, `feedparser`, etc., are available. `DockerOperator` provides better isolation.
*   **State Management:** DAG 2 relies on the list of health IDs created by DAG 1. Storing this in S3/GCS is robust. Airflow Variables work for smaller lists but are less ideal for potentially large lists or complex state.
*   **RSS Feed Parsing:** This is the most uncertain part. You need to investigate the Eurostat RSS feed structure to reliably extract updated dataset IDs. It might require string matching or looking for specific tags. Error handling here is critical. What if the feed format changes?
*   **Data Format:** Strongly consider using **Parquet** for the processed output (`PROCESSED_DATA_PATH`) instead of CSV. It's columnar, compressed, and much more efficient for analytics platforms. Modify `jsonParser.py`'s output stage using `pandas.DataFrame.to_parquet` or `pyarrow`.
*   **Parsing Parallelism:** For the `parse_updated_task`, the number of updated files might be small, so parallel parsing might not be necessary. For the `initial_bulk_parse_task`, parallelism is likely beneficial.
*   **Atomicity:** Consider what happens if `download_updated_task` succeeds but `parse_updated_task` fails. Your raw data is updated, but the processed data isn't. Retrying the parse task should fix it. Ensure the parsing function can safely re-run on an already downloaded file.
*   **Error Handling:** Configure retries on tasks. Add logging within your Python functions. Consider Airflow alerts for failures.
*   **Configuration:** Use Airflow Variables/Connections for `RAW_DATA_PATH`, `PROCESSED_DATA_PATH`, `HEALTH_ID_LIST_PATH`, API details, RSS URL, etc.

This two-DAG approach cleanly separates the infrequent setup/bulk load from the frequent update cycle, making the pipeline easier to manage and reason about. Remember to focus on robust error handling and making the update process reliable, especially the RSS feed parsing.
