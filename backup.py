import boto3
import botocore
import toml
import os
import sys
import threading
import readchar
import math
from tabulate import tabulate
from datetime import datetime

LINE_CLEAR = '\x1b[2K'

class ProgressPercentage(object):
    def __init__(self, filename, count, total):
        self._filename = filename
        self._count = str(count)
        self._total = str(total)
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        print(end=LINE_CLEAR)
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            mibytes_seen_so_far = bytes_readout(self._seen_so_far)
            mibytes_size = bytes_readout(self._size)
            sys.stdout.write("\r[%s/%s] %s (%s/%s, %.2f%%)" % (self._count.rjust(len(self._total)), self._total, self._filename, mibytes_seen_so_far, mibytes_size, percentage))
            sys.stdout.flush()


def bytes_readout(size_bytes):
   if size_bytes == 0:
       return "0 B"
   size_name = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p)
   return "%s %s" % (s, size_name[i])


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
total_size = sum(os.path.getsize(os.path.join(config['directory_name'], f)) for f in files_diff)
print(f"{len(files_diff)} ({bytes_readout(total_size)}) files to upload")
if(len(files_diff) > 0):
    while(True):
        key = query_key("Action? [V]iew, [U]pload, [Q]uit ")

        if key == 'v':
            tabulated_data = []
            for file in files_diff:
                true_file = os.path.join(config['directory_name'], file)
                time = os.path.getmtime(true_file)
                tabulated_data.append([file, bytes_readout(os.path.getsize(true_file)), datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')])

            print(tabulate(tabulated_data, headers=["File name", "Size", "Last modified"]))
        elif key == 'u':        
            error = []
            count = 0
            for file in files_diff:
                count += 1
                true_file = os.path.join(config['directory_name'], file)
                try:
                    s3.upload_file(true_file, config['bucket_name'], file,
                        Callback=ProgressPercentage(true_file, count, len(files_diff)),
                        ExtraArgs = {
                            'StorageClass': 'DEEP_ARCHIVE'
                        }
                    )
                except Exception as e:
                    print(end=LINE_CLEAR)
                    sys.stdout.write(f"\rUpload of {file} failed")
                    if hasattr(e, 'response') and 'Error' in e.response and 'Code' in e.response['Error']:
                        error.append([file, e.response['Error']['Code']])
                    else:
                        error.append([file, type(e).__name__])
                finally:
                    print()
            if (len(error) > 0):
                print(tabulate(error, headers=["File name", "Error"]))
                print(f"{len(error)} files failed to upload")
            os.remove(file_tree_location)
            break
        elif key == 'q':
            break
