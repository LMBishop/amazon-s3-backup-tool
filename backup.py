#!/usr/bin/env python3

import boto3
import toml
import os
import core.util as util
import core.backup as backup
import sys

s3 = boto3.client('s3')

config_location = os.path.join(os.path.expanduser("~"), '.aws-s3-backup', 'config')

def add_backup_configuration(config):
    backup_name = input("Enter backup name: ")
    bucket_name = input("Enter AWS bucket name to backup to: ")
    directory_name = input("Enter absolute path to directory to backup: ")

    config['backups'].append(
        {
            'name': backup_name,
            'bucket_name': bucket_name,
            'directory_name': directory_name,
            'file_tree_location': f'{bucket_name}-file-tree'
        }
    )

    dir_name = os.path.dirname(config_location)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    with open(config_location, 'w') as f:
        toml.dump(config, f)

    print(f"Configuration saved to {config_location}")


if not os.path.exists(config_location):
    if len(sys.argv) > 1:
        print("No backups are configured, re-run without arguments to configure")
        sys.exit(1)
    add_backup_configuration({'backups': []})


def view_backups(config):
    count = 0
    for backup_config in config['backups']:
        count += 1
        print(f"{count}. Backup '{backup_config['name']}' to {backup_config['bucket_name']}")
        print(f"Directory: {backup_config['directory_name']}")
        file_tree_location = os.path.join(os.path.expanduser("~"), '.aws-s3-backup', backup_config['file_tree_location'])
        if not os.path.exists(file_tree_location):
            print("No file tree found")
            continue
        print(f"File tree location: {file_tree_location}")
        file_tree = toml.load(file_tree_location)
        file_tree_set = set(file_tree['files'])
        print(f"{len(file_tree_set)} files in file tree")


def start_backup(config, interactive):
    for backup_config in config['backups']:
        print(f"Backup '{backup_config['name']}' ({backup_config['bucket_name']})")
        file_tree_location = os.path.join(os.path.expanduser("~"), '.aws-s3-backup', backup_config['file_tree_location'])

        if (not os.path.exists(file_tree_location)) or (interactive and util.query_yes_no(f"Update file tree for {backup_config['bucket_name']} from AWS?")):
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=backup_config['bucket_name'])
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
        if backup.do_backup(s3, backup_config, file_tree_set, interactive=interactive):
            os.remove(file_tree_location)

config = toml.load(config_location)

if len(sys.argv) > 1:
    if sys.argv[1] == 'backup':
        start_backup(config, False)
    elif sys.argv[1] == 'add':
        add_backup_configuration(toml.load(config_location))
    elif sys.argv[1] == 'view':
        view_backups(config)
    else:
        print(f"Invalid command '{sys.argv[1]}'")
        sys.exit(1)
else:
    while True:
        key = util.query_key("Action? [C]heck for backups, [A]dd new backup, [V]iew current backups, [Q]uit ")

        if key == 'c':
            start_backup(config, True)
        elif key == 'a':
            add_backup_configuration(config)
        elif key == 'v':
            view_backups(config)
        elif key == 'q':
            break
