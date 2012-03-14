"""
Retrieve private IPs of EC2 instances that have tag 'log'.

Should set access_keys to /etc/boto.cfg before use this script.

Reference: http://www.saltycrane.com/blog/2010/03/how-list-attributes-ec2-instance-python-and-boto/
"""

from boto import ec2

def servers_with_tag_log(region=None): # region in ec2.regions()
    servers = []
    conn = ec2.connect_to_region(region or current_region())
    if not conn:
        return servers
    reservations = conn.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    for i in instances:
        if 'log' in i.tags:
            servers.append(i.private_ip_address)
    return servers

def current_region():
    # http://stackoverflow.com/questions/4249488/find-region-from-within-ec2-instance
    import json
    import urllib2
    query = urllib2.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document')
    doc = json.loads(query.read())
    return doc['region']

if __name__ == '__main__':
    regions = {
        '0': 'ap-northeast-1',
        '1': 'us-east-1',
        '2': 'ap-southeast-1',
        }
    import sys
    region = sys.argv[1]
    if region in regions:
        region = regions[region]
    print servers_with_tag_log(region)

