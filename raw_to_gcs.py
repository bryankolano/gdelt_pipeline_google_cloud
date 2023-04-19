import pandas as pd
from pandas.errors import EmptyDataError
from prefect import task, flow
import prefect_gcp
from datetime import datetime
from urllib.request import urlretrieve
from urllib.error import HTTPError
import os
import glob
import pathlib
from prefect_gcp.cloud_storage import GcsBucket




@task(retries = 3, log_prints=True)
def download_gdelt(date:datetime, hour, minute) -> pd.DataFrame:
    current_date = f"{date.year}{date.month:02d}{date.day:02d}"


    #URL format is below
    #http://data.gdeltproject.org/gdeltv2/20230308220000.export.CSV.zip
    url = f'http://data.gdeltproject.org/gdeltv2/{date.year}{date.month:02d}{date.day:02d}{hour:02d}{minute:02d}00.export.CSV.zip'
    
    #The returned CSV comes back as tab separated
    current_df = pd.read_csv(url, compression= 'zip', sep = '\t' )

    return current_df





@task(log_prints= True, retries= 3)
def send_to_gcs(df, date:datetime, hour, minute) -> str:
    #Using Prefect Orion Server (localhost:4200), I created a block where I connected my GCS service account and bucket 
    #so I could easily make a connection to that bucket through Orin
    gdelt_bucket = GcsBucket.load("gdelt")
    
    
    #take in a dataframe, and write it to the GCS bucket, and return the GCS folder path
    gdelt_bucket.upload_from_dataframe(df = df,
                                       to_path = f'{date.year}/{date.month:02d}/{date.day:02d}/gdelt_events_{date.year}_{date.month:02d}_{date.day:02d}_{hour:02d}_{minute:02d}.csv' )
    #return GCS path
    return f"gdelt_events_{date.year}_{date.month:02d}_{date.day:02d}_{hour:02d}_{minute:02d} written to GCS bucket."

@flow(log_prints=True)
def collect_to_gcs(date:datetime, hour, minute):
    
    #current grab takes it's given arguments, created the URL, downloads CSV in memory, turns into dataframe, and returns dataframe
    current_grab = download_gdelt(date, hour, minute)

    #this function takes in the output of the download function, and sends that dataframe to the Google Cloud Storage bucket.
    send_to_gcs(current_grab, date, hour, minute)


if __name__=="__main__":
     
    daterange = pd.date_range("2023/01/01", "2023/03/31")

    for date in daterange:
        for hour in range(0,24):
            for minute in [0,15,30,45]:
                try:
                    collect_to_gcs(date, hour, minute)
                except HTTPError as err:
                    #Typically the CSV "endpoints" are updated every 15 minutes, but not always
                    #as such, if one is missing, an HTTP error will be thrown
                    #So this try/ except catches that error, write the file name to no_data.txt for later verification
                    with open('no_data.txt', 'a') as f:
                        f.write(f"No data for:{date.year}_{date.month:02d}_{date.day:02d}_{hour:02d}_{minute:02d}")
                        f.write("\n")
                    continue
                except EmptyDataError as err2:
                #Sometimes these "endpoints" have downloadable CSVs, so the the HTTPError exception won't catch it,
                #but then turns out there is nothing in the CSV so a EmptyDataError is found.
                #This except catches that.
                    with open('no_data.txt', 'a') as f:
                        f.write(f"No data in file for:{date.year}_{date.month:02d}_{date.day:02d}_{hour:02d}_{minute:02d}")
                        f.write("\n")
                    continue

    


