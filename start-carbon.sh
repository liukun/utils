python /home/ec2-user/utils/rm_stale_pid_file.py /opt/graphite/storage/carbon-$1-a.pid carbon-$1.py
exec /opt/graphite/bin/carbon-$1.py --debug start 
