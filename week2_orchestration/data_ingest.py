from airflow import DAG
from datetime import datetime,timezone
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta
from airflow.operators.python import PythonOperator
import pyarrow as pa
import os
import pandas as pd
import psycopg2
import boto3

def downloadparquet(ds,root_path,baseurl):
    
   url = baseurl + ds[0:-3] + ".parquet"
   
   data = pd.read_parquet(path = url)
  
   print("The datatypes is", data.dtypes)
   
   data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date
   
   data['year'] = data['tpep_pickup_datetime'].dt.year

   data['month']  = data['tpep_pickup_datetime'].dt.month
  
   print("The datatypes after the change is" , data.dtypes)

   data = data[ (data['year'] == int(ds[0:4])) & (data['month'] == int(ds[5:7])) ]

   table = pa.Table.from_pandas(data)
   
   s3Fs = pa.fs.S3FileSystem( region = "us-east-1" )
   
   s3Client = boto3.client('s3')
   s3path = root_path + "/" + ds[0:-3] + "/"
   splitpath = s3path.strip("s3://").split("/")
   bucket = splitpath[0]
   prefix = splitpath[1]+"/"+splitpath[2]
   response = s3Client.list_objects(Bucket = bucket,Prefix = prefix)
   
   if 'Contents' in response:
        for folder in response['Contents']:
            s3Client.delete_object(Bucket = bucket,Key = folder['Key'])
            

   #pq.write_to_dataset(table, root_path = root_path , partition_cols = ['tpep_pickup_date'],filesystem = s3Fs)
   

   data.to_parquet(path = root_path + "/" + ds[0:-3], partition_cols = ['tpep_pickup_date'])

   print("The execution date is",ds)
   print("The yearmonth is" ,ds[0:-3])
dag = DAG(
    dag_id = "uploaded-files",
    start_date = datetime(year = 2021,month=1,day=1,hour = 0,minute = 0,second = 0,tzinfo=timezone.utc),
    schedule = "0 0 1 * *",
    end_date = datetime(year = 2021 , month = 1, day =1 , hour = 0, minute = 0,second = 0,tzinfo = timezone.utc)
)



task1 = PythonOperator(
    
    dag = dag,
    task_id = "filedownloadtask",
    python_callable = downloadparquet,
    op_kwargs = {"ds" : '{{ ds }}' , "root_path" : "s3://debucketnew1/nycyellowtaxidata", "baseurl" : "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"  }
)

def retreiveRedshiftParams():
    
    host = os.environ["redshifthost"]
    port = os.environ["port"]
    database = os.environ["database"]
    user = os.environ["user"]
    password = os.environ["password"] 
    
    return {"host":host, "port":port ,"database":database ,"user":user ,"password":password}

def connecttoredshift(s3path,ds):
   
    host = os.environ["redshifthost"]
    port = os.environ["port"]
    database = os.environ["database"]
    user = os.environ["user"]
    password = os.environ["password"]
    
    
    
    s3path = s3path + "/" + ds[0:-3] + "/"
    print(s3path)
    conn = psycopg2.connect(host = host, database = database, port = port ,user = user, password = password)
    
    cursor = conn.cursor()
    
    cursor.execute("""

              create external schema IF NOT EXISTS nyc_schema
              from data catalog
              database 'nyc_db'
              region 'us-east1'
              iam_role 'arn:aws:iam:::role/service-role/AmazonRedshift-CommandsAccessRole-20240313T104444'
              create external database if not exists;

            """)

    conn.commit()
    #cursor.execute("""DROP EXTERNAL TABLE IF EXISTS nyc_db.nyc_schema.nycyellowtaxi""")
    #cursor.execute(f"""CREATE EXTERNAL TABLE nyc_schema.nycyellowtaxidata (
        
#tpep_pickup_datetime           TIMESTAMP,
#tpep_dropoff_datetime          TIMESTAMP,
#passenger_count                DOUBLE PRECISION,
#trip_distance                  DOUBLE PRECISION,
#RatecodeID                     DOUBLE PRECISION,
#store_and_fwd_flag             VARCHAR(256),
#PULocationID                   BIGINT,
#DOLocationID                   BIGINT,
#payment_type                   BIGINT,
#fare_amount                    DOUBLE PRECISION,
#extra                          DOUBLE PRECISION,
#mta_tax                        DOUBLE PRECISION,
#tip_amount                     DOUBLE PRECISION,
#tolls_amount                   DOUBLE PRECISION,
#improvement_surcharge          DOUBLE PRECISION,
#total_amount                   DOUBLE PRECISION,
#congestion_surcharge           DOUBLE PRECISION,
#airport_fee                    DOUBLE PRECISION,
#year                           BIGINT,
#month                          BIGINT)

#PARTITIONED BY (tpep_pickup_date DATE)
#STORED AS parquet
#LOCATION '{s3path}'


#""")
    #result = cursor.fetchall()
   # print(result)
    
    #conn.commit()
    
    s3Client = boto3.client('s3')
   
    splitpath = s3path.strip("s3://").split("/") 
    bucket = splitpath[0]
    prefix = splitpath[1]+"/"+splitpath[2]
    response = s3Client.list_objects(Bucket = bucket,Prefix = prefix)
    print(response)
    folders = []
    if 'Contents' in response:
        for folder in response['Contents']:
            folders.append(folder['Key'])

    conn.autocommit = True
    for folder in folders:
         print("folder",folder)
         splitfolder = folder.split("/")[-2]
         cursor.execute(f"""
                   ALTER TABLE nyc_schema.nycyellowtaxidata
                   ADD PARTITION(tpep_pickup_date='{splitfolder.split("=")[-1]}')
                   LOCATION '{s3path}{splitfolder}'
                   
                   """)
         

    cursor.close()
    conn.close()


task2 = PythonOperator(
    dag = dag,
    task_id = "fetchtheenvironmentvariables",
    python_callable = retreiveRedshiftParams
)

task3 = PythonOperator(

    dag = dag,
    task_id = "writetoredshiftDatabase",
    python_callable = connecttoredshift,
    op_kwargs = {"ds" : '{{ ds }}', "s3path" : f"s3://***/nycyellowtaxidata"}

)

task1 >> task2 >> task3
