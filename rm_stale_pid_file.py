"""
Remove stale '.pid' file if process not running.
Based on Perl script from http://devel.ringlet.net/sysutils/stalepid/
 but check `process_name` with in the `COMMAND` not force being equal.
"""

import os
import subprocess
import sys

if len(sys.argv) < 3:
    print 'WRONG! Usage:', sys.argv[0], 'pid_filename process_name'
    sys.exit(1)

pid_file = sys.argv[1]
process_name = sys.argv[2]

if not os.path.isfile(pid_file):
    print 'PASS.', pid_file, 'not found.'
    sys.exit(0)
pid = None
with open(pid_file) as f:
    pid = int(f.read().strip())

# will use subprocess.check_output in Python 2.7
# for now just follow http://stackoverflow.com/questions/4814970
output = subprocess.Popen(['ps', '-p', str(pid), '-o', 'command'], stdout=subprocess.PIPE).communicate()[0]
assert output.startswith('COMMAND')
if process_name in output:
    print 'PASS.', process_name, 'found.'
    sys.exit(0)

# the process is not running or not contains process_name
os.unlink(pid_file)
print 'PASS. Removed', pid_file

