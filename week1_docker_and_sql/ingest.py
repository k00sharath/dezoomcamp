from sqlalchemy import engine
import pandas as pd
import os
import time
import argparse
def ingest(params):
    username=params.username
    host=params.host
    port=params.port
    database=params.database
    password=params.password
    url=params.url
    tablename=params.tablename
    parq_path="taxi.parquet"
    csv_filepath="taxidata.csv"
    conn=engine.create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
    
    os.system(f"wget {url} -O {parq_path}")
    df=pd.read_parquet(f"./{parq_path}")
    df.to_csv(csv_filepath,index=False)
    df_csv=pd.read_csv(f"./{csv_filepath}",iterator=True,chunksize=100000)
    df.head(n=0).to_sql(name=tablename,con=conn,if_exists="replace")
    first_df=next(df_csv)
    iter_count=1
    while not first_df.empty:
        start_time=time.time()
        first_df.to_sql(name=tablename,con=conn,if_exists="append")
        first_df=next(df_csv,pd.DataFrame())
        end_time=time.time()
        print(f"Time for iteration {iter_count} is {end_time-start_time} seconds")
        iter_count+=1

if __name__=="__main__":
    
    argparser=argparse.ArgumentParser(description="ingest data into postgres")
    argparser.add_argument("--username",help="username")
    argparser.add_argument("--host",help="host name")
    argparser.add_argument("--port",help="port number")
    argparser.add_argument("--database",help="database name")
    argparser.add_argument("--password",help="password for database")
    argparser.add_argument("--url",help="url link for file download")
    argparser.add_argument("--tablename",help="table name")
    params=argparser.parse_args()
    ingest(params)
