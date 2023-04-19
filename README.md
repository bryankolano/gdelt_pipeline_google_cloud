# ETL Pipeline of GDELT data using Prefect and Google Cloud Platform (GCP)
## A series of python scripts that grab data from GDELT database and write to GCP
## By: Bryan Kolano, Original repo creation: March 20th, 2023


***

## Background
Getting into data engineering has made me start to explore various ETL tools.  In the past few months, I started to learn a little about the ETL tool: Prefect.  I have used GDELT in the past for a class I used to teach in R.  I thought GDELT would be a good dataset to use in a pipeline with Prefect.

## Files in this repo
- The first script in the repo is called `raw_to_gcs.py`.  It grabs each subset of GDELT data (GDELT data us updated every 15 minutes, with some 15 minutes increments that don't exist/ have no data.)  This script take the raw CSVs it grabs and stores them in a Google Cloud Storage (GCS) bucket.  This initial script grabbed all GDELT data from January 1st, 2023 until March 31st, 2023.  It grabbed all the "historic" data.

- The next script is called `raw_to_gcs_daily.py`.  After running the historical script and gathering all data since the start of 2023, this script was scheduled to run each day through CRON scheduler, and would grab the previous day's GDELT data.


- The next script, called `gcs_to_bq.py` connects to the GCS bucket, and grabs CSVs one at a time.  It does variables transformations and data cleaning operations on the data, and then pushes each individual CSV to Google's dataware house platform: Big Query.  Inside Big Query, a SQL table is appended with each day's data after it is cleaned.  Before I wrote a daily script to move the previous days' data, I wrote this script to move all data in the GCS bucket from January 1st, 2023 through March 31st, 2023.  

- The last script, called `gcs_to_bq_daily.py` is similar to the `gcs_to_bq.py` script, except that instead of moving all data since the start of 2023, it only moves the previous day's.  It was set up to run daily using CRON, and would run after the `raw_to_gcs_daily.py` script. 


- `no_data.txt`  is a text file that is written to each time a particular GDELT dataset does not exist.  Or sometimes the file exists, so the CSV will be downloaded, but it turns out there is nothing in the CSV.  Somes days, there are at least three 15 minute increments that do not have data uploaded to GDELT's site.  The `raw_to_gcs.py` script writes which sections of data do not exist so that quality checks can be performed manually to confirm these data don't dont exist.

- `requirements.txt`

## Data cleaning
I discovered that there are some data quality issues in the GDELT data.  Some of the issues I found and fixed include:

* I could not find a source that properly listed all 61 columns names.  Some sources I found had either 60 or 62, but the data has 61 columns.  I had to do some exploration to line the columns up properly.
* Examine and the drop the columns that were mostly blank.
* Some observations were labeled wrong instead of NA, but with things like 'unnamed'.
* Some lat/ longs had multiple decimals.  For example, more than one read something like -106.57.23.
* Set all columns types so they matches the data.
* Fill NAs and blanks.
* Some of the CSV "endpoints" did not exist, so had to account for that.
* Some of the CSV "endpoints" existed, but the downloaded CSVs contained no data; that had to be account for too.

## Steps for Prefect

To take advantage of the functionality of Prefect, the following Prefect steps were taken: 
1. start Prefect Orion server
2. Create Google Credentials block
3. Create Google Cloud Storage block
4. Create Google Big Query block
5. Create Prefect deployment
6. Schedule Prefect deployment to run each day for previous day's data
