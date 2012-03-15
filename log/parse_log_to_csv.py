import bz2
import csv
import os
import re
import sys

DEBUG = len(sys.argv) > 1 and sys.argv[1] == 'debug'

prefix = '/backup/tower_log_csv/tower.'
log_path = '/backup/tower_log_backup/'

import pycassa
pool = pycassa.ConnectionPool('TowerSpace')
family = pycassa.ColumnFamily(pool, 'Log')

class Parser:
    cate = ''
    req = ''
    pattern = re.compile("(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s")
    csv = None
    csv_file = None
    csv_name = None

    def parse(self, string):
        if self.csv is None: return
        if not self.req in string: return
        res = self.pattern.match(string)
        if res: self.deal_data(res.groupdict())

    def deal_data(self, res):
        raise NotImplementedError()

    def new_csv(self, date, region):
        global prefix
        name = prefix + '.'.join([self.cate, date, region, 'csv'])
        self.close()
        if os.path.isfile(name):
            if DEBUG:
                print ' skip', name
            return
        self.csv_name = name
        self.csv_file = open(name+'.tmp', 'wb');
        self.csv = csv.writer(self.csv_file, dialect='excel')
        self.prepare_data()

    def prepare_data(self):
        pass

    def close(self):
        if self.csv_file is not None:
            self.clear_data()
            self.csv = None
            self.csv_file.close()
            os.rename(self.csv_name+'.tmp', self.csv_name)
            self.csv_file = None

    def clear_data(self):
        pass

class SignUp(Parser):
    cate = 'SignUp'
    req = 'cate:SignUp sub:done'

    def deal_data(self, res):
        player = res['player']
        date = res['date']
        self.csv.writerow([player, date])
        family.insert(player, {'SignUp': date})

class FloorCount(Parser):
    cate = 'FloorCount'
    req = 'floorCount:'
    pattern = re.compile("(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*id:(?P=player)[,\]].*floorCount:(?P<floor>[0-9]+?)[,\]]")

    def prepare_data(self):
        self.data = {}

    def deal_data(self, res):
        player = res.pop('player')
        self.data[player] = res

    def clear_data(self):
        if not getattr(self, 'data', None): return
        for player in self.data:
            res = self.data[player]
            sign_up = ''
            try:
                key = 'SignUp'
                sign_up = family.get(player, [key])[key]
            except pycassa.NotFoundException, e:
                pass
            self.csv.writerow([player, res['floor'], res['date'], sign_up])
        self.data.clear()

parsers = [SignUp(), FloorCount()]

for root, dirs, files in os.walk(log_path):
    while dirs: dirs.pop()
    region = None
    date = None
    files.sort()
    for name in files:
        parts = name.split('.')
        if parts[-1] != 'bz2': continue
        need = False
        if parts[1] != date or parts[2] != region:
            date = parts[1]
            region = parts[2]
            for parser in parsers:
                parser.new_csv(date, region)
                if parser.csv:
                    need = True
        else:
            for parser in parsers:
                if parser.csv:
                    need = True
                    break
        if not need: continue
        f = bz2.BZ2File(os.path.join(root, name))
        for line in f:
            for parser in parsers:
                parser.parse(line)
        f.close()

for parser in parsers:
    parser.close()
