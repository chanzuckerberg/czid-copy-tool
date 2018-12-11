#!/usr/bin/env python3

import datetime
import boto3
import botocore
import subprocess

s3_bucket = "idseq-database"
s3_top_folder = "ncbi-sources"
s3 = boto3.resource("s3")

remote_server = "ftp.ncbi.nih.gov"
files_to_download = [
    # "/blast/db/FASTA/nt.gz",
    # "/blast/db/FASTA/nr.gz",
    "/pub/taxonomy/taxdump.tar.gz",
]
folders_to_download = []
# folders_to_download = ["/pub/taxonomy/accession2taxid"]


def main():
    start_copy_flow()


def start_copy_flow():
    today_tag = datetime.datetime.today().strftime("%Y-%m-%d")
    try:
        # Don't run if the done file is there already
        s3.Object(s3_bucket, f"{s3_top_folder}/{today_tag}/done").load()
        print("Done file exists already")
    except botocore.exceptions.ClientError as e:
        print(f"File doesn't exist: {e}")

    for file in files_to_download:
        download_file(f"{remote_server}{file}")

    for folder in folders_to_download:
        download_folder(f"{remote_server}{folder}")

    for file in files_to_download:
        upload_file(file)


def download_file(file):
    subprocess.call(f"wget -P temp -nv {file}", shell=True)


def download_folder(folder):
    subprocess.call(f"wget -P temp -rnv -np -nH --cut-dirs=2 -e robots=off -R 'index.html*' {folder}", shell=True)


def upload_file(file):
    today_tag = datetime.datetime.today().strftime("%Y-%m-%d")
    s3.Bucket(s3_bucket).upload_file(f"temp/{file}", f"{s3_top_folder}/{today_tag}")


main()
