`amazon-s3-backup-tool` is a script I wrote to assist with my backups to Amazon S3 buckets. It will check a given directory against a cached list of files in a S3 bucket, and if there is a file name present locally which isn't remote, it will upload it to Amazon. 

It is hard-coded to use the Deep Archive storage class, for relatively inexpensive backups (less than a tenth of a cent per GB at time of writing).

## Usage

Run `backup.py` to configure it. Configurations are stored at `~/.aws-s3-backup`. 

Run `backup.py backup` to start a backup non-interactively. Otherwise, simply run `backup.py` with no args to get an interactive session.

