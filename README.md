# AWS-Metar-Python-Airflow-ETL-Pipeline


## Overview
This project is an Airflow ETL (Extract, Transform, Load) pipeline designed to extract METAR data, process it, and upload the results to an S3 bucket. The ETL process involves scraping METAR data from NOAA, parsing and processing the data, and finally storing the results in a CSV file on AWS S3.

## Components

### `dag.py`
The main Airflow DAG (Directed Acyclic Graph) definition file. It defines tasks for running the ETL process and uploading the data to S3.

### `etl.py`
The ETL module contains functions for scraping METAR data, parsing and processing the data, and saving the results in a CSV file. It also includes functions for wind direction, wind speed, wind variability, prevailing visibility, temperature, dewpoint, cloud layers, and altimeter setting.

### `EC2_Commands.sh`
This script contains commands to set up the necessary dependencies on an AWS EC2 instance. It installs Python, Apache Airflow, and required Python libraries.

## Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/Aakaaaassh/AWS-Metar-Python-Airflow-ETL-Pipeline.git
   cd AWS-Metar-Python-Airflow-ETL-Pipeline
2. **Install Dependencies:**
   Run the following commands on your EC2 instance.
   ```bash
   sudo sh EC2_Commands.sh
3. **Configure Airflow:**
   Set up your Airflow environment by following the Airflow configuration steps. Ensure that your Airflow environment has the necessary connections and variables set up.
4. **Run the ETL Pipeline:**
   Trigger the DAG in Airflow to start the ETL process.


## Usage

- Ensure that your Airflow environment is properly configured with the required connections and variables.
- Trigger the metar_etl_and_upload_to_s3 DAG in Airflow to initiate the ETL process.

## License
This project is licensed under the MIT License   
