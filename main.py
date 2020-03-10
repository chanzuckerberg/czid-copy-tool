#!/usr/bin/env python3

'''
 To run this script with aegea do:

aegea batch submit --command="cd /mnt; git clone https://github.com/chanzuckerberg/idseq-copy-tool.git; cd idseq-copy-tool; pip3 install schedule; python3 main.py "  --storage /mnt=500 --volume-type gp2 --ecr-image idseq_dag --memory 120000 --queue idseq-prod-lomem --vcpus 16 --job-role idseq-pipeline

'''

import argparse
import boto3
import botocore
import datetime
import os
import schedule
import subprocess
import time

s3_bucket = "idseq-database"
s3_top_folder = "ncbi-sources"
s3 = boto3.resource("s3")

remote_server = "ftp.ncbi.nih.gov"
files_to_download = [
    "/blast/db/FASTA/nt.gz",
    "/blast/db/FASTA/nt.gz.md5",
    "/blast/db/FASTA/nr.gz",
    "/blast/db/FASTA/nr.gz.md5",
    "/pub/taxonomy/taxdump.tar.gz",
    "/pub/taxonomy/taxdump.tar.gz.md5",
]

files_to_unzip = set([
    "/blast/db/FASTA/nt.gz",
    "/blast/db/FASTA/nr.gz"
    "/pub/taxonomy/taxdump.tar.gz",
])
folders_to_download = [
    "/pub/taxonomy/accession2taxid",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-as-daemom', dest='run_as_daemon', action='store_true')
    parser.set_defaults(run_as_daemon=False)

    start_copy_flow()

    args = parser.parse_args()

    if args.run_as_daemon: # Infinite loop
        schedule.every(7).days.do(start_copy_flow)
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
        print(f"s3://{s3_bucket}/{dated_subfolder}/done")
        return
    except botocore.exceptions.ClientError:
        print(f"Done file doesn't exist. Should run.")

    try:
        for file in files_to_download:
            download_file(f"{remote_server}{file}")

        for folder in folders_to_download:
            download_folder(f"{remote_server}{folder}")

        for file in files_to_download:
            upload_temp_file(file, dated_subfolder, file in files_to_unzip)

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


def upload_temp_file(file, dated_subfolder, unzip):
    file = os.path.basename(file)
    src = f"temp/{file}"
    dst = f"{dated_subfolder}/{file}"
    upload_file(src, dst)
    if unzip: # the file
        # A lot of assumptions here. main assumption is that the unzipped file is src[:-3]
        command_execute(f"gunzip {src}")
        upload_file(src[:-3], dst[:-3])
        # Upload a lz4 copy because they are more reliable to subsequently download
        lz4_out = lz4(src[:-3])
        upload_file(lz4_out, f"{dated_subfolder}/{os.path.basename(lz4_out)}")


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


def lz4(src):
    # See also idseq_dag/steps/generate_lz4.py
    dst = src + '.lz4'
    print(f"Compressing {src} to lz4 ...")
    command_execute(f'lz4 -9 -f {src} {dst}')
    return dst


if __name__ == '__main__':
    main()
