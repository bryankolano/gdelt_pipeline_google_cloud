import pandas as pd
import pandas_gbq 
import pyarrow
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from datetime import datetime, timedelta
from pathlib import Path
import os
import re





@task(log_prints=True)
def grab_from_gcs(date:datetime) -> Path:
    #Using Prefect Orion server, I created a Prefect block to connect to a GCS bucket with bucket name and service account info.
    gcs_bucket = GcsBucket.load("gdelt")

    #Download the bucket contents to the local file path
    gcs_bucket.download_folder_to_path(from_folder = f"{date.year}/{date.month:02d}/{date.day:02d}",
                                        to_folder = f"data/{date.year}/{date.month:02d}/{date.day:02d}")

    #return the path of the donwloaded content
    return Path(f"data/{date.year}/{date.month:02d}/{date.day:02d}")
    

@task(log_prints=True)
def transform_combine(path:Path) -> pd.DataFrame:

    #The CSV's do not come with a header row.  So they needed to be defined when read in
    cols = 'globaleventid sqldate monthyear year fractiondate actor1code actor1name actor1countrycode actor1knowngroupcode \
    actor1ethniccode actor1religion1code actor1religion2code actor1type1code actor1type2code actor1type3code actor2code \
    actor2name actor2countrycode actor2knowngroupcode actor2ethniccode actor2religion1code actor2religion2code actor2type1code \
    actor2type2code actor2type3code isrootevent eventcode eventbasecode eventrootcode quadclass goldsteinscale nummentions \
    numsources numarticles avgtone actor1geotype actor1geofullname actor1geocountrycode actor1geoadm1code actor1geoadm2code \
    actor1geolat actor1geolong actor1geofeatureid actor2geotype actor2geofullname actor2geocountrycode actor2geoadm1code \
    actor2geoadm2code actor2geolat actor2geolong actor2geofeatureid actiongeotype actiongeofullname actiongeocountrycode \
    actiongeoadm1code actiongeoadm2code actiongeolat actiongeolong actiongeofeatureid dateadded sourceurl'

    cols = cols.split()

    current_df = pd.read_csv(path, names= cols)

    #After some exploration, these columns were always empty or almost always empty
    current_df = current_df.drop(columns=['actor1knowngroupcode', 'actor1ethniccode', 'actor1religion1code', 'actor1religion2code',
        'actor1type1code','actor1type2code', 'actor1type3code', 'actor2knowngroupcode', 'actor2ethniccode',
        'actor2religion1code', 'actor2religion2code', 'actor2type1code', 'actor2type2code', 'actor2type3code' ])

    #Some of the CSVs have data quality issues.  In some, when read into python, they did not have values for 
    #lat/ longs, but rather said things like "Unnamed: 51" or "Unnamed: 6"
    #This first pattern below looks for those particular data issues
    pat = re.compile(pattern= 'Unnamed: -?\d+')

    #In some of the lat/ long column, it appears someone fat fingers some values in, insert values that have two decimals
    # for example: -106.500.23 for a latitude
    #this pattern looks for this error
    pat2 = re.compile(pattern= '-?\d+\.\d+\.\d')

    #For the columns that have these data entry errors/ data quality issues, turn them into string, and then we can drop
    #particular rows when certain conditions are met
    current_df = current_df.astype({
        'actor2geolong' : str,
        'actor1geolong' : str,
        'actor2geolat' : str,
        'actor1geolat' : str,
    })

    #If any of these columns contain the patterns identified above, drop that row from the dataset
    current_df = current_df.drop(current_df[current_df['actor1geolat'].str.contains(pat)].index)
    current_df = current_df.drop(current_df[current_df['actor1geolat'].str.contains(pat2)].index)
    current_df = current_df.drop(current_df[current_df['actor2geolat'].str.contains(pat)].index)
    current_df = current_df.drop(current_df[current_df['actor2geolat'].str.contains(pat2)].index)
    current_df = current_df.drop(current_df[current_df['actor1geolong'].str.contains(pat)].index)
    current_df = current_df.drop(current_df[current_df['actor1geolong'].str.contains(pat2)].index)
    current_df = current_df.drop(current_df[current_df['actor2geolong'].str.contains(pat)].index)
    current_df = current_df.drop(current_df[current_df['actor2geolong'].str.contains(pat2)].index)


    #change data types for datetime columns
    #Pandas struggled to choose some of the data types, and then could be saved in parquet (for uploading to Big Query)
    #therefore, some of the columns had to be forced to certain data types.
    #Additionally, for the columns I turned into strings to run the regex patterns against, I need to turn them back into their
    #original data types
    current_df['sqldate'] = pd.to_datetime(current_df['sqldate'], format= "%Y%m%d")
    current_df['monthyear'] = pd.to_datetime(current_df['monthyear'], format = "%Y%m")
    current_df['year'] =  pd.to_datetime(current_df['year'], format = "%Y")
    current_df['isrootevent'] = current_df['isrootevent'].map({0:False, 1:True})
    current_df['eventbasecode'] = current_df['eventbasecode'].astype(int)
    current_df['quadclass'] = current_df['quadclass'].astype(int)
    current_df['actor2geotype'] = current_df['actor2geotype'].astype(int)
    current_df['actor1geotype'] = current_df['actor1geotype'].astype(int)
    current_df['actor1geolong'] = current_df['actor1geolong'].astype(float)
    current_df['actor2geolong'] = current_df['actor2geolong'].astype(float)
    current_df['actor1geolat'] = current_df['actor1geolat'].astype(float)
    current_df['actor2geolat'] = current_df['actor2geolat'].astype(float)
    current_df['nummentions'] = current_df['nummentions'].astype(int)

    #for all the string columns with "NAs" or "NaN", fill them with ""
    for col in current_df.select_dtypes(include=[object]).columns:
        current_df[col] = current_df[col].fillna("")

    #for the integer and float columns, fill their NAs with 0
    for col in current_df.select_dtypes(include= [int, float]).columns:
        current_df[col] = current_df[col].fillna(0)



    return current_df

@task(log_prints=True)
def to_bq(df:pd.DataFrame) -> str:

    gcp_credentials_block = GcpCredentials.load("gdelt-cred")

    #take the current DF, use the Prefect block to establish connection with the pre-created Big Query table, and then write
    # the df to Big Query.
    pandas_gbq.to_gbq(dataframe= df,
                destination_table= 'bk_gdelt.gdelt_collection',
                project_id='dtc-de-375700',
                credentials=  gcp_credentials_block.get_credentials_from_service_account(),
               if_exists= 'replace',
                api_method="load_csv")



#
@flow(log_prints=True)
def gcs_transform_bq(date:datetime):
    
    
    
    folder_path = grab_from_gcs(date)

    for file in os.listdir(folder_path):

        current_file_path = os.path.join(folder_path, file)
        current_df = transform_combine(current_file_path)

        to_bq(current_df)

        os.remove(current_file_path)

if __name__=="__main__":

    yesterday = datetime.today() - timedelta(days = 1)
    
    gcs_transform_bq(yesterday)
