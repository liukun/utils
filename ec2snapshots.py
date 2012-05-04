'''Manage EC2 EBS Snapshots

Try to keep only one snapshot within each interval between 0, 1 hour, 3 hours, 7 hours, 15 hours, ..., 2^N-1 hours. Will remove the oldest unnecessary one if there are more then N copies. (N == KEEP_SNAPSHOTS)
'''
import datetime
import math
import re
import sys

'''
sudo pip install boto
'''
import boto
import boto.ec2

import pid_file
lock = pid_file.pid_file(sys.argv[0]+'.pid').acquire()
assert lock

CRON = 'cron' in sys.argv
REGION = 'us-west-1'
VOLS = set()
KEEP_SNAPSHOTS = 10

for arg in sys.argv:
    if arg.startswith('vol-'): VOLS.add(arg)
    elif arg.startswith('region:'): REGION = arg.replace('region:', '', 1)

assert VOLS
conn = boto.ec2.EC2Connection(region=boto.ec2.get_region(REGION))
vols = conn.get_all_volumes(list(VOLS))
if not CRON: print 'volumes:', vols

for vol in vols:
    snapshots = vol.snapshots()
    snapshots = sorted(snapshots, key=(lambda x: x.start_time), reverse=True)
    had = set()
    rm = None
    log2 = math.log(2)
    for shot in snapshots:
        # http://stackoverflow.com/questions/127803/how-to-parse-iso-formatted-date-in-python
        start_time = datetime.datetime(*map(int, re.split('[^\d]', shot.start_time)[:6]))
        passed = datetime.datetime.utcnow() - start_time
        hours = passed.days * 24 + passed.seconds / 3600
        if hours < 0: continue
        n = int(math.floor(math.log1p(hours)/log2))
        if n >= KEEP_SNAPSHOTS or n in had:
            if not rm: rm = shot
        else:
            had.add(n)
    if not CRON: print 'had snapshots (in hours):', [pow(2,i+1)-1 for i in sorted(had)]
    if 0 not in had:
        vol.create_snapshot()
        had.add(0)
        if not CRON: print 'created a snapshot'
    if rm and len(had) > KEEP_SNAPSHOTS:
        rm.delete()
        if not CRON: print 'deleted', rm

