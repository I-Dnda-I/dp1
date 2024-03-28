import sys
import json
import time
import schedule
import pandas as pd
from pathlib import Path
from ftplib import FTP_TLS
from os import environ, remove
from datetime import datetime

def get_gtp()-> FTP_TLS:
    FTPHOST = environ["FTPHOST"]
    FTPUSER = environ["FTPUSER"]
    FTPPASS = environ["FTPPASSWORD"]

    print(f"test: {FTPHOST}")
    
    ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
    ftp.prot_p()
    return ftp

def upload_to_ftp(ftp:FTP_TLS, file_source:Path):
    with open(file_source, "rb") as fp:
        ftp.storbinary(f"STOR {file_source.name}", fp)

def delete_file(file_source: str | Path) -> None:
    remove(file_source)

def read_csv(config: dict) -> pd.DataFrame:
    url = config["URL"]
    params = config["PARAMS"]
    return pd.read_csv(url, **params)

def pipeline():
    with open("config.json","r") as fp:
        config = json.load(fp)
    ftp = get_gtp()
    
    #print(read_csv(config["SDN"]).head())
    for source_name, source_config in config.items():
        file_name = source_name + ".CSV"
        df = read_csv(config[source_name]) 
        print(f"file {file_name} is downloaded", datetime.now().strftime("%H:%M:%S"))
        df.to_csv(file_name, index=False)
        print(f"file {file_name} is created locally", datetime.now().strftime("%H:%M:%S"))
        
        upload_to_ftp(ftp, Path(file_name))
        print(f"file {file_name} is uploaded to server", datetime.now().strftime("%H:%M:%S"))

        delete_file(file_name)
        print(f"file {file_name} is deleted locally", datetime.now().strftime("%H:%M:%S"))

if __name__ == "__main__":
    param = sys.argv[1]
    if param == "manual":
        pipeline()
    elif param == "schedule":
        schedule.every().day.at("03:32").do(pipeline)
        while True:
            schedule.run_pending()
            time.sleep(60)