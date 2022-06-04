import threading
import readchar
import math
import os
import sys

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
