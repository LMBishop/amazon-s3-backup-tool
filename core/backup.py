from tabulate import tabulate
import core.util as util
from datetime import datetime
import os
import sys

def do_backup(s3, config, file_tree, interactive=True):
    local_files = []

    for dirpath, dirnames, filenames in os.walk(config['directory_name']):
        for file in filenames:
            relpath = os.path.relpath(dirpath, config['directory_name']).replace('\\','/')
            if relpath == '.':
                local_files.append(file)
            else:
                local_files.append(f"{relpath}/{file}")

    files_diff = list(filter(lambda x: x not in file_tree, local_files))
    total_size = sum(os.path.getsize(os.path.join(config['directory_name'], f)) for f in files_diff)

    if(len(files_diff) > 0):
        while(True):
            print(f"{len(files_diff)} ({util.bytes_readout(total_size)}) files to upload")
            key = util.query_key("Action? [V]iew, [U]pload, [Q]uit ") if interactive else 'u'

            if key == 'v':
                tabulated_data = []
                for file in files_diff:
                    true_file = os.path.join(config['directory_name'], file)
                    time = os.path.getmtime(true_file)
                    tabulated_data.append([file, util.bytes_readout(os.path.getsize(true_file)), datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')])

                print(tabulate(tabulated_data, headers=["File name", "Size", "Last modified"]))
            elif key == 'u': 
                print(f"Starting backup '{config['name']}' to {config['bucket_name']}")
                error = []
                count = 0
                for file in files_diff:
                    count += 1
                    true_file = os.path.join(config['directory_name'], file)
                    try:
                        s3.upload_file(true_file, config['bucket_name'], file,
                            Callback=util.ProgressPercentage(true_file, count, len(files_diff)),
                            ExtraArgs = {
                                'StorageClass': 'DEEP_ARCHIVE'
                            }
                        )
                    except Exception as e:
                        print(end=util.LINE_CLEAR)
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
                return True
            elif key == 'q':
                break
    else:
        print("No files to upload")
    
    return False