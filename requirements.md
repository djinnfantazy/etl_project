Task 1: Setup and Preprocessing

1.1 DON'T STORE API KEYS IN CODE, USE ENV VARIABLES
1.2 Use poetry framework for project configuration

Task 2: Ingest Data to the Landing Zone

2.1. Choose a public API to ingest data from. The following options are provided, but the student can choose any other API they prefer:
  •  TransportAPI (https://developer.transportapi.com/): Provides real-time public transport data (buses, trains, etc.) with frequent updates.
  •  NewsAPI(https://newsapi.org/): Provides real-time news data from various news outlets with new articles and headlines being added frequently.
  •  Alternatively, the student can choose another dynamic API of their choice that provides frequent updates.
2.2. Fetch the latest data from the selected API and save it to the landing_zone directory.
2.3. The landing zone should always hold only the current batch of ingested data (overwrite the existing file each time).
2.4. Add logging to track the success or failure of the API call and include timestamps for debugging.

Task 3: Load Data to the Bronze Layer

3.1. Create a bronze directory to store raw, unprocessed data (delta format)
3.2. Apply a schema to the data to ensure consistency (e.g., using StructType in PySpark).
3.3. Save the data in a partitioned manner (e.g., by timestamp or date) to allow incremental loading.
3.4. Implement a process to keep history by saving the newly ingested data as well as previously ingested data. Use a partitioning strategy in Spark (e.g., by ingest_date).
3.5. Add logging for successful or failed bronze layer data writes.

Task 4: Data Integration into the Silver Layer

4.1. Read data from the bronze layer.
4.2. Perform data transformations to clean the data, such as:
  •  Renaming columns to match the desired schema.
  •  Flattening JSON structures if needed (e.g., extracting nested fields).
  •  Handling missing or inconsistent values.
  •  Schema application: Ensure data adheres to a consistent schema.
4.3. Implement different scenarios for loading:
  •  Upsert: For data that should be updated if it already exists.
  •  Overwrite: For complete replacement of existing data.
  •  SCD Type 2 (Slowly Changing Dimension): For capturing historical changes in data (e.g., when a piece of data changes over time).
4.4. Write the transformed data to the silver layer (denormalized and cleaned).

Task 5: Data Aggregation and Transformation for the Gold Layer

5.1. Create a gold layer that contains aggregated and transformed data suitable for analysis.
5.2. Perform the following transformations:
  •  Join multiple datasets (e.g., combining weather data with transport data).
  •  Aggregate data (e.g., calculating averages, sums, counts).
5.3. Write the denormalized data to the gold layer for easier analysis.

Task 6: Data Validation and Testing

6.1. Test the data pipeline:
  •  Unit tests: Write unit tests for each transformation function (e.g., data cleansing, renaming, flattening JSON).
  •  Integration tests: Test the end-to-end pipeline (data ingestion, loading, transformations, and saving).
6.2. Validate data integrity:
  •  Ensure the data is correct and that transformations work as expected (e.g., values match expected ranges, no duplicates).
  •  Monitor for data quality issues like missing values or incorrect formats and log them.