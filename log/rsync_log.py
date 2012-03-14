"""
"""
import os
import subprocess
import sys

import servers_to_log

data_path = '/data/logger/'
compressed_dir = 'compressed'

aws_current_region = None

def do_rsync_on_servers(log_type):
    for IP in servers_to_log.servers_with_tag_log():
        do_rsync(IP, log_type)

def do_rsync(IP, log_type):
    subprocess.call(['rsync', '-ah', IP+'::'+log_type,
        os.path.join(data_path, log_type, IP+os.path.sep)])

def do_compress(log_type):
    root_path = os.path.join(data_path, log_type)
    compressed_path = os.path.join(root_path, compressed_dir)
    if not os.path.isdir(compressed_path):
        os.makedirs(compressed_path)
    for root, dirs, files in os.walk(root_path):
        while dirs:
            ip = dirs.pop()
            if ip == compressed_dir: continue
            for sub, subdirs, files in os.walk(os.path.join(root_path, ip)):
                for f in files:
                    source = os.path.join(sub, f)
                    target = os.path.join(compressed_path, _compressed_name(f, ip))
                    _bzip2(source, target)

def _compressed_name(origin_file_name, ip):
    global aws_current_region
    if aws_current_region is None:
        aws_current_region = servers_to_log.current_region()
    parts = origin_file_name.split('.')
    assert parts[1].startswith('20') # should be 2012, 2013, ...
    ip = 'ip-' + ip.replace('.', '-')
    return '.'.join([parts[0], parts[1], aws_current_region, ip, 'log', 'bz2'])

def _bzip2(source_file, target_file):
    if os.path.isfile(target_file):
        return
    tmp_file = target_file + '.tmp'
    devnull = open(os.devnull, 'w')
    cat = 'zcat' if source_file.endswith('.gz') else 'cat'
    cat = subprocess.Popen([cat, source_file], stdout=subprocess.PIPE)
    bzip = subprocess.Popen(["bzip2"], stdin=cat.stdout, stdout=subprocess.PIPE)
    cat.stdout.close()
    tee = subprocess.Popen(["tee", tmp_file], stdin=bzip.stdout, stdout=devnull)
    bzip.stdout.close()
    tee.wait()
    devnull.close()
    os.rename(tmp_file, target_file)

if __name__ == '__main__':
    log_type = sys.argv[1]
    do_compress(log_type)
    do_rsync_on_servers(log_type)
