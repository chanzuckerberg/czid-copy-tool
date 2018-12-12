#!/usr/bin/env python3

import os
import datetime
import boto3
import botocore
import subprocess
import schedule
import time

s3_bucket = "idseq-database"
s3_top_folder = "ncbi-sources"
s3 = boto3.resource("s3")

remote_server = "ftp.ncbi.nih.gov"
files_to_download = [
    "/blast/db/FASTA/nt.gz",
    "/blast/db/FASTA/nr.gz",
    "/pub/taxonomy/taxdump.tar.gz",
]
folders_to_download = ["/pub/taxonomy/accession2taxid"]
# folders_to_download = ["/pub/taxonomy/biocollections"]


def main():
    start_copy_flow()

    schedule.every(12).hours.do(start_copy_flow)
    print("Scheduler running...")

    while True:
        schedule.run_pending()
        time.sleep(1)


def start_copy_flow():
    date_tag = datetime.datetime.today().strftime("%Y-%m-%d")
    dated_subfolder = f"{s3_top_folder}/{date_tag}"
    print(f"Dated subfolder: {dated_subfolder}")

    try:
        # Don't run if the done file is there already
        s3.Object(s3_bucket, f"{dated_subfolder}/done").load()
        print("Done file exists already. Skipping this run.")
        return
    except botocore.exceptions.ClientError:
        print(f"Done file doesn't exist. Should run.")

    try:
        for file in files_to_download:
            download_file(f"{remote_server}{file}")

        for folder in folders_to_download:
            download_folder(f"{remote_server}{folder}")

        for file in files_to_download:
            upload_temp_file(file, dated_subfolder)

        for folder in folders_to_download:
            upload_temp_folder(folder, dated_subfolder)

        write_done_file(dated_subfolder)

        print(f"Copy flow finished successfully.")
    except RuntimeError as e:
        print(f"Error in the copy flow. Aborting run. {e}")


def download_file(file):
    print(f"Downloading file {file} ...")
    cmd = f"wget -P temp -cnv {file}"
    command_execute(cmd)


def download_folder(folder):
    print(f"Downloading folder {folder} ...")
    # wget to folder 'temp', no verbose, don't follow parent links,
    # don't include host name, cut out 2 middle directories, ignore
    # robots.txt, ignore index.html
    cmd = f"wget -P temp -crnv -np -nH --cut-dirs=2 -e robots=off -R 'index.html*' {folder}/"
    command_execute(cmd)


def command_execute(cmd):
    print(f"Command: {cmd}")
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.wait()
    stdout, stderr = proc.communicate()
    stdout = stdout.decode("utf-8")
    stderr = stderr.decode("utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"Command error: {stderr}")


def upload_temp_file(file, dated_subfolder):
    file = os.path.basename(file)
    src = f"temp/{file}"
    dst = f"{dated_subfolder}/{file}"
    upload_file(src, dst)


def upload_file(src, dst):
    print(f"Uploading {src} ...")
    s3.Bucket(s3_bucket).upload_file(src, dst)


def upload_temp_folder(folder, dated_subfolder):
    folder = os.path.basename(folder)
    for base in os.listdir(f"temp/{folder}"):
        src = f"temp/{folder}/{base}"
        dst = f"{dated_subfolder}/{folder}/{base}"
        upload_file(src, dst)


def write_done_file(dated_subfolder):
    print(f"Uploading done file ...")
    s3.Object(s3_bucket, f"{dated_subfolder}/done").put(Body="")


main()
