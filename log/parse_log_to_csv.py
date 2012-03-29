import bz2
import csv
import os
import re
import sys
from operator import itemgetter

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
if DEBUG:
    prefix = 'out.'
    log_path = './'

if not DEBUG:
    import pycassa
    pool = pycassa.ConnectionPool('TowerSpace')
    family = pycassa.ColumnFamily(pool, 'Log')
def cassa_insert_if_not_exists(region, key, column, value):
    if DEBUG: return
    key = region + key
    if family.get_count(key, columns=[column]): return
    family.insert(key, {column: value})
def cassa_get(region, key, column, default):
    if DEBUG: return default
    key = region + key
    try:
        return family.get(key, [column])[column]
    except pycassa.NotFoundException, e:
        return default

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
        if res:
            res = res.groupdict()
            # SignIn/recv SignUp/recv do not have correct ID
            if int(res['player']) > 0:
                self.deal_data(res, line)

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
        self.region = region
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
        cassa_insert_if_not_exists(self.region, player, 'SignUp', date)

class IAP(Parser):
    cate = 'IAP'
    req = 'cate:InAppPurchase sub:correct'
    pattern = re.compile('(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*cate:InAppPurchase sub:correct.*"diamond",(?P<value>[0-9]+?),')

    def deal_data(self, res, line):
        date = res['date']
        player = res['player']
        self.csv.writerow([date, player, res['value']])
        cassa_insert_if_not_exists(self.region, player, 'FirstIAP', date)

class FirstPurchaseAfterIAP(Parser):
    cate = 'FirstPurchaseAfterIAP'
    pt_purchase = 'cate:InAppPurchase sub:correct'
    pt_change = 'cate:ChangeDiamond'
    pt_minus = re.compile('sub:(?P<sub>\w+) json:\["delta","(?P<delta>-\d+)",')

    def prepare_data(self):
        self.data = set()

    def deal_data(self, res, line):
        date = res['date']
        player = res['player']
        if self.pt_purchase in line:
            self.data.add(player)
        elif self.pt_change in line and player in self.data:
            res = pt_minus.search(line)
            if not res: return
            res = res.groupdict()
            self.csv.writerow([date, player, res['sub'], res['delta']])
            self.data.remove(player)

class ScratchCardReward(Parser):
    cate = 'ScratchCardReward'
    req = 'sub:scratchCard'
    pattern = re.compile('(?P<date>.*?)\s\[INFO\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*cate:Change(?P<type>.*) sub:scratchCard.*"delta","(?P<value>[0-9]+?)",')

    def deal_data(self, res, line):
        self.csv.writerow([res['date'], res['player'], res['type'], res['value']])

class Daily(Parser):
    cate = 'Daily'
    req = 'ACTIVITY'
    pt_SimplePlayerInfo = re.compile('SimplePlayerInfo\(.* bux:(?P<bux>\d+), floorCount:(?P<floor>\d+), diamond:(?P<diamond>\d+)')
    pt_satScratchCard = 'cate:ScratchCard sub:sat'
    pt_delta = re.compile('cate:(?P<cate>\w+) sub:(?P<sub>\w+) json:\["delta","(?P<delta>-?\d+)",')
    categories = {
        'ChangeDiamond': [
            'buyDiamondsScratchCards', 'buyGoldsScratchCards',
            'restockSpeedUp', 'sellSpeedUp', 'buyGangster',
            'buyEquip', 'upgradeElevator', 'constructSpeedUp',
            ],
        }

    def prepare_data(self):
        self.data = {}

    def deal_data(self, res, line):
        player = res['player']
        last = res['date']
        res = self.data.setdefault(player, {})
        if 'last' not in res or res['last'] < last:
            res['last'] = last
        if last > res.get('last_player_info', ''):
            player_info = self.pt_SimplePlayerInfo.search(line)
            if player_info:
                player_info = player_info.groupdict()
                res['last_player_info'] = last
                for key in ('floor', 'bux', 'diamond'):
                    res[key] = player_info[key]
        if self.pt_satScratchCard in line:
            res['satCard'] = res.get('satCard', 0) + 1
        delta_info = self.pt_delta.search(line)
        if delta_info:
            delta_info = delta_info.groupdict()
            key = ':'.join([delta_info['cate'], delta_info['sub']])
            res[key] = res.get(key, 0) + 1
            delta = int(delta_info['delta'])
            key = delta_info['cate'] + ('Plus' if delta > 0 else 'Neg')
            res[key] = res.get(key, 0) + delta

    def clear_data(self):
        if not getattr(self, 'data', None): return
        row = ['id', 'last_time', 'sign_up_time']
        keys = ['floor', 'satCard', 'bux', 'ChangeBuxPlus', 'ChangeBuxNeg',
                'diamond', 'ChangeDiamondPlus', 'ChangeDiamondNeg',
                ]
        for c in self.categories:
            for k in self.categories[c]:
                keys.append(c + ':' + k)
        self.csv.writerow(row+keys)
        for player in self.data:
            res = self.data[player]
            sign_up = cassa_get(self.region, player, 'SignUp', '')
            row = [player, res['last'], sign_up]
            for k in keys:
                row.append(res.get(k, 0))
            self.csv.writerow(row)
        self.data.clear()

parsers = [SignUp(), IAP(), ScratchCardReward(), Daily()]

def batch_process(files, date, region):
    '''batch process one day's data'''
    if not files: return
    try:
        if not date or not region:
            print ' skipped', files
            return
        need = False
        for parser in parsers:
            parser.new_csv(date, region)
            if parser.csv:
                need = True
        if not need:
            return
        _batch_process(files, date, region)
    finally:
        del files[:]

def _batch_process(files, date, region):
    if DEBUG: print ' processing', files
    opens = []
    for f in files:
        opens.append(bz2.BZ2File(os.path.join(root, f)))
    lines = []
    for f in opens:
        try:
            lines.append(f.next())
        except StopIteration:
            pass
    while lines:
        # find most early line of log
        index, line = min(enumerate(lines), key=itemgetter(1))
        for parser in parsers:
            parser.parse(line)
        try:
            next = opens[index].next()
            lines[index] = next
        except StopIteration:
            del lines[index]
            opens[index].close()
            del opens[index]
    for parser in parsers:
        parser.close()

for root, dirs, files in os.walk(log_path):
    while dirs: dirs.pop()
    last_date = None
    last_region = None
    batch = []
    region = None
    date = None
    files.sort()
    while files:
        f = files.pop(0)
        # f == 'activity.2012-03-26.us-east-1.ip-10-212-178-114.log.bz2'
        parts = f.split('.')
        if parts[-1] != 'bz2': continue
        if (last_date != parts[1] or last_region != parts[2]):
            batch_process(batch, last_date, last_region)
            last_date = parts[1]
            last_region = parts[2]
        batch.append(f)
    batch_process(batch, last_date, last_region)
