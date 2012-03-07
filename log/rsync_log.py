"""
"""
import subprocess
import sys

import servers_to_log

def do_rsync_on_servers(region, log_type):
    for IP in servers_to_log.get_list(region):
        do_rsync(IP, log_type)

def do_rsync(IP, log_type):
    subprocess.call(['rsync', '-ah', IP+'::'+log_type,
        '/data/logger/'+log_type+'/'+IP+'/'])

if __name__ == '__main__':
    do_rsync_on_servers(sys.argv[1], sys.argv[2])
