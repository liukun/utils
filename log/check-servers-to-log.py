"""
Retrieve private IPs of EC2 instances that have tag 'log'.

Should set access_keys to /etc/boto.cfg before use this script.

Reference: http://www.saltycrane.com/blog/2010/03/how-list-attributes-ec2-instance-python-and-boto/
"""

from boto import ec2

def get_list(region): # region in ec2.regions()
    conn = ec2.connect_to_region(region)
    reservations = conn.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    servers = []
    for i in instances:
        if 'log' in i.tags:
            servers.append(i.private_ip_address)
    return servers

if __name__ == '__main__':
    print get_list('ap-northeast-1')

