import boto3
import botocore
import toml
import os
import sys
import threading
import readchar
from tabulate import tabulate
from datetime import datetime

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write("\r%s  %.2f%%" % (self._filename, percentage))
            sys.stdout.flush()


def query_key(question):
    print(f"{question}", end="")
    sys.stdout.flush()
    key = readchar.readkey().lower()
    print()
    return key


def query_yes_no(question):
    return query_key(f"{question} [Y/n] ") != 'n'

s3 = boto3.client('s3')

# s3.upload_file(
#     'hugefile2', 'lmbishop-test', 'hugefile2',
#     Callback=ProgressPercentage('hugefile2')
# )

config_location = os.path.join(os.path.expanduser("~"), '.aws-s3-backup', 'config')
file_tree_location = os.path.join(os.path.expanduser("~"), '.aws-s3-backup', 'file-tree')

if not os.path.exists(config_location):
    bucket_name = input("Enter backup bucket name: ")
    directory_name = input("Enter directory to backup: ")

    config = {
        'bucket_name': bucket_name,
        'directory_name': directory_name
    }

    os.makedirs(os.path.dirname(config_location))
    with open(config_location, 'w') as f:
        toml.dump(config, f)

    print(f"Configuration saved to {config_location}")

config = toml.load(config_location)

if (not os.path.exists(file_tree_location)) or query_yes_no("Update file tree from AWS?"):
    res = s3.list_objects(Bucket=config['bucket_name'])
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=config['bucket_name'])
    file_tree_array = []
    for page in pages:
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            file_tree_array.append(obj['Key'])

    file_tree = {
        'files': file_tree_array,
    }
    with open(file_tree_location, 'w') as f:
        toml.dump(file_tree, f)

file_tree = toml.load(file_tree_location)

file_tree_set = set(file_tree['files'])

local_files = []

for dirpath, dirnames, filenames in os.walk(config['directory_name']):
    for file in filenames:
        relpath = os.path.relpath(dirpath, config['directory_name']).replace('\\','/')
        if relpath == '.':
            local_files.append(file)
        else:
            local_files.append(f"{relpath}/{file}")

files_diff = list(filter(lambda x: x not in file_tree_set, local_files))
print(f"{len(files_diff)} files to upload")
if(len(files_diff) > 0):
    while(True):
        key = query_key("Action? [V]iew, [U]pload, [Q]uit ")

        if key == 'v':
            tabulated_data = []
            for file in files_diff:
                time = os.path.getmtime(os.path.join(config['directory_name'], file));
                tabulated_data.append([file, datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')])

            print(tabulate(tabulated_data, headers=["File name", "Last modified"]))
        elif key == 'u':        
            error = []    
            for file in files_diff:
                true_file = os.path.join(config['directory_name'], file)
                try:
                    s3.upload_file(true_file, config['bucket_name'], file,
                        Callback=ProgressPercentage(true_file),
                        ExtraArgs = {
                            'StorageClass': 'DEEP_ARCHIVE'
                        }
                    )
                except botocore.exceptions.ClientError as e:
                    sys.stdout.write(f"\rUpload of {file} failed")
                    print()
                    error.append([file, e.response['Error']['Code']])
                    continue
                print()
            if (len(error) > 0):
                print(f"{len(error)} files failed to upload")
                print(tabulate(error, headers=["File name", "Error"]))
            os.remove(file_tree_location)
            break
        elif key == 'q':
            break
