import boto3
import toml
import os
import sys
import threading
import readchar

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
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def query_yes_no(question):
    print(f"{question} [Y/n] ", end="")
    sys.stdout.flush()
    key = readchar.readkey().lower()
    print()
    return key != 'n'


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
    objects = s3.list_objects(Bucket=config['bucket_name'])['Contents']
    file_tree_array = list(map(lambda x: x['Key'], objects))
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
print(files_diff)