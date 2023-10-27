# tm_application
Arvin Escolano's attempt of becoming one of the renowned Thinking Machines' Data Operations Engineer

## Data Extraction, Transformation, and Loading Solution via Python, Google BigQuery, and Streamlit

### 1. Environment

#### 1.1. Environment Setup
Python/Streamlit Environment: The project utilizes Python for data extraction and transformation and loading in a web service application known as Streamlit. A streamlit application can be created based from a github repository. 

Python Libraries: The project relies on some Python libraries, and their requirements are listed in requirements.txt. 

Google Cloud Platform (GCP): The project uses Google BigQuery for data storage. You will need to set up a GCP project, enable the BigQuery API, and create a service account to obtain the necessary credentials (JSON key file).

Streamlit: Streamlit is used to create the web service application.

#### 1.2. Configuration
Configuration Files: The project uses a configuration file: config.json. This only stores the dataset name and the table name for the GBQ Bigquery table. The service account details are securely stored in the secrets.toml file of Streamlit.

### 2. Data Architecture
   
#### 2.1. Data Flow Overview
The data architecture of the ETL (Extract, Transform, Load) process consists of three main components: Data Extraction, Data Transformation, and Loading in the Web Service/Application.

#### 2.2. Components

##### 2.2.1. Data Extraction
CSV Data Source: The raw data is sourced from a CSV file stored on Google Drive.
Custom Data Loader: A custom Python function, load_csv, is used to load the data from the CSV source and parse timestamps.
Custom Timestamp Parser: Another custom function, custom_parser, handles different timestamp formats during the data extraction process.

##### 2.2.2. Data Transformation
Correction Dictionary Generation: A custom Python function, generate_correction_dict, constructs a correction dictionary to clean project names based on phonetic similarity (soundex) and similarity scores (fuzzy matching).
Data Cleaning: Data cleaning is performed by the clean_df function, which drops null values, ensures hours are greater than 0, and applies the correction dictionary.
Date Formatting: The make_naive function ensures that datetime values are in naive format for consistency.

##### 2.2.3. Data Loading and Loading
Google BigQuery: Cleaned data is loaded into Google BigQuery using the bq_write function, and the table is partitioned by day.
Streamlit Web Service: The Streamlit framework is used to create a user-friendly web service for accessing and visualizing the check-in data.
Querying BigQuery Data: Streamlit uses the query_bq_table function to query data from Google BigQuery in real-time and display it in the web service.

#### 2.3. Data Storage
Google BigQuery: Google BigQuery is used to store the cleaned data. Tables are partitioned by day to optimize query performance.

### 3. Conclusion
This documentation provides an overview of the project's setup, data architecture, and components involved in the ETL process using the indicated tools. The Streamlit app can be accessed through the link https://arvinappliestotm.streamlit.app/.
