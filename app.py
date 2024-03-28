import sys
import json
import time
import schedule
import pandas as pd
from pathlib import Path
from ftplib import FTP_TLS
from os import environ, remove

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

def read_csv(config: dict) -> pd.DataFrame:
    url = config["URL"]
    params = config["PARAMS"]
    return pd.read_csv(url, **params)

if __name__ == "__main__":
    with open("config.json","r") as fp:
        config = json.load(fp)
    ftp = get_gtp()
    
    #print(read_csv(config["SDN"]).head())
    for source_name, source_config in config.items():
        file_name = source_name + ".CSV"
        df = read_csv(config[source_name]) 
        df.to_csv(file_name, index=False)
        
        upload_to_ftp(ftp, Path(file_name))