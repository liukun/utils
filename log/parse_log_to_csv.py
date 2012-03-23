import bz2
import csv
import os
import re
import sys

# assure one instance
this_file = sys.argv[0]
sys.path.append(os.path.join(os.path.dirname(this_file), '..'))
pid_file = __import__('pid_file')
del sys.path[-1]
lock = pid_file.pid_file(this_file+'.pid').acquire()
assert lock

# print debug info
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

    def parse(self, line):
        if self.csv is None: return
        if not self.req in line: return
        res = self.pattern.match(line)
        if res: self.deal_data(res.groupdict(), line)

    def deal_data(self, res, line):
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

    def deal_data(self, res, line):
        player = res['player']
        date = res['date']
        self.csv.writerow([player, date])
        family.insert(player, {'SignUp': date})

class IAP(Parser):
    cate = 'IAP'
    req = 'cate:InAppPurchase sub:correct'
    pattern = re.compile('(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*cate:InAppPurchase sub:correct.*"diamond",(?P<value>[0-9]+?),')

    def deal_data(self, res, line):
        self.csv.writerow([res['date'], res['player'], res['value']])

class ScratchCardReward(Parser):
    cate = 'ScratchCardReward'
    req = 'sub:scratchCard'
    pattern = re.compile('(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*cate:Change(?P<type>.*) sub:scratchCard.*"delta","(?P<value>[0-9]+?)",')

    def deal_data(self, res, line):
        self.csv.writerow([res['date'], res['player'], res['type'], res['value']])

class Daily(Parser):
    cate = 'Daily'
    req = 'ACTIVITY'
#    pattern = re.compile("(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*id:(?P=player)[,\]].*floorCount:(?P<floor>[0-9]+?)[,\]]")
    pt_floor = re.compile('floorCount:(?P<value>[0-9]+?)[,\]]')
    pt_buyDiaScratchCard = re.compile('cate:ChangeDiamond sub:buyDiamondsScratchCards.*(?P<value>delta)')
    pt_buyBuxScratchCard = re.compile('cate:ChangeDiamond sub:buyGoldsScratchCards.*(?P<value>delta)')
    pt_satScratchCard = 'cate:ScratchCard sub:sat'

    def prepare_data(self):
        self.data = {}

    def deal_data(self, res, line):
        player = res['player']
        last = res['date']
        res = self.data.setdefault(player, {})
        if 'last' not in res or res['last'] < last:
            res['last'] = last
        v = self._value_of(self.pt_floor, line)
        if v:
            s = res.get('floor', 0)
            v = int(v)
            if v > s:
                res['floor'] = v
        v = self._value_of(self.pt_buyDiaScratchCard, line)
        if v: res['buyDia'] = res.get('buyDia', 0) + 1
        v = self._value_of(self.pt_buyBuxScratchCard, line)
        if v: res['buyBux'] = res.get('buyBux', 0) + 1
        if self.pt_satScratchCard in line:
            res['satCard'] = res.get('satCard', 0) + 1

    def _value_of(self, p, line):
        res = p.search(line)
        if not res: return None
        return res.groupdict()['value']

    def clear_data(self):
        if not getattr(self, 'data', None): return
        self.csv.writerow(['id', 'last_time', 'floor', 'buyDiamondScratchCards', 'buyBuxScratchCards', 'satisScratchCards', 'sign_up_time'])
        for player in self.data:
            res = self.data[player]
            sign_up = ''
            try:
                key = 'SignUp'
                sign_up = family.get(player, [key])[key]
            except pycassa.NotFoundException, e:
                pass
            self.csv.writerow([player, res['last'], res.get('floor', 0), res.get('buyDia', 0), res.get('buyBux', 0), res.get('satCard', 0), sign_up])
        self.data.clear()

parsers = [SignUp(), IAP(), ScratchCardReward(), Daily()]

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
