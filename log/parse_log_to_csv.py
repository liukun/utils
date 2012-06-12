import bz2
import csv
import datetime
import json
import os
import re
import sys
from operator import itemgetter

reload(sys) 
sys.setdefaultencoding('utf-8')

# assure one instance
this_file = sys.argv[0]
sys.path.append(os.path.join(os.path.dirname(this_file), '..'))
pid_file = __import__('pid_file')
del sys.path[-1]
lock = pid_file.pid_file(this_file+'.pid').acquire()
assert lock

# print debug info
DEBUG = 'debug' in sys.argv

prefix = '/backup/tower_log_csv/tower.'
log_path = '/backup/tower_log_backup/'
# if DEBUG:
#     prefix = 'out.'
#     log_path = './'

if not DEBUG:
    import pycassa
    pool = pycassa.ConnectionPool('TowerSpace')
    family = pycassa.ColumnFamily(pool, 'Log')
def cassa_insert_if_not_exists(region, key, column, value):
    if DEBUG: return
    key = region + key
    if family.get_count(key, columns=[column]): return
    family.insert(key, {column: value})
def cassa_insert(region, key, column, value):
    if DEBUG: return
    key = region + key
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
    pattern = re.compile("(?P<date>.*?)\s\[(INFO|WARN)\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s")
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
        self.load_relative(date, region)
        if not self.csv_name: return
        self.csv_file = open(name+'.tmp', 'wb');
        self.csv = csv.writer(self.csv_file, dialect='excel')
        self.region = region
        self.prepare_data()

    def load_relative(self, date, region):
        pass

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
    pattern = re.compile('(?P<date>.*?)\s\[(INFO|WARN)\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*cate:InAppPurchase sub:correct.*"diamond",(?P<value>[0-9]+?),')

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
            res = self.pt_minus.search(line)
            if not res: return
            res = res.groupdict()
            self.csv.writerow([date, player, res['sub'], res['delta']])
            self.data.remove(player)

class ScratchCardReward(Parser):
    cate = 'ScratchCardReward'
    req = 'sub:scratchCard'
    pattern = re.compile('(?P<date>.*?)\s\[(INFO|WARN)\].*ACTIVITY\splayer:(?P<player>[0-9]+?)\s.*cate:Change(?P<type>.*) sub:scratchCard.*"delta","(?P<value>[0-9]+?)",')

    def deal_data(self, res, line):
        self.csv.writerow([res['date'], res['player'], res['type'], res['value']])

class Session(Parser):
    cate = 'Session'
    req = 'cate:Sign'
    pattern = re.compile('(?P<date>.*?)\s\[(INFO|WARN)\].*ACTIVITY\splayer:(?P<player>\d+?) .* session:(?P<session>\d+) .* cate:Sign(?P<method>\w+) ')

    def prepare_data(self):
        self.cache = {}
        self.data = {}

    def deal_data(self, res, line):
        player = res['player']
        date = res['date']
        method = res['method']
        if method == 'In':
            self.cache[player] = res
        elif method == 'Out':
            last = self.cache.get(player, None)
            if not last: return
            old_date = datetime.datetime.strptime(last['date'], '%Y-%m-%d %H:%M:%S,%f')
            new_date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
            delta = new_date - old_date
            delta = delta.days * 86400 + delta.seconds
            if delta > 0:
                data = self.data.setdefault(player, {})
                data['times'] = data.get('times', 0) + 1
                data['seconds'] = data.get('seconds', 0) + delta

    def clear_data(self):
        for player in self.data:
            data = self.data[player]
            self.csv.writerow([player, data['times'], data['seconds']])
        self.data.clear()

class Daily(Parser):
    cate = 'Daily'
    req = 'ACTIVITY'
    pt_SimplePlayerInfo = re.compile('SimplePlayerInfo\(.* bux:(?P<bux>\d+), floorCount:(?P<floor>\d+), diamond:(?P<diamond>\d+),')
    pt_satScratchCard = 'cate:ScratchCard sub:sat'
    pt_delta = re.compile('cate:(?P<cate>\w+) sub:(?P<sub>\w+) json:\["delta","(?P<delta>-?\d+)",')
    pt_times = re.compile('cate:(?P<cate>Floor|Gangster|Friend) sub:(?P<sub>\w+)')
    pt_dream = re.compile('DreamJobs:(?P<dream>\d+)')
    pt_friends = re.compile('cate:Friend sub:status.*"FriendsNum:(?P<num>\d+)"')
    categories = {
        'ChangeDiamond': [
            'buyDiamondsScratchCards', 'buyGoldsScratchCards',
            'restockSpeedUp', 'sellSpeedUp', 'buyGangster',
            'buyEquip', 'upgradeElevator', 'constructSpeedUp',
            ],
        'Floor': [
            'built', 'construct', 'destroy'],
        'Gangster': [
            'reside', 'evict'],
        'Friend': [
            'invite', 'accept', 'refuse', 'delete'],
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
        info = self.pt_times.search(line)
        if info:
            info = info.groupdict()
            key = ':'.join([info['cate'], info['sub']])
            res[key] = res.get(key, 0) + 1
        info = self.pt_dream.search(line)
        if info:
            info = info.groupdict()
            key = 'dream'
            res[key] = info[key]
        info = self.pt_friends.search(line)
        if info:
            info = info.groupdict()
            key = 'friends'
            res[key] = info['num']

    def clear_data(self):
        if not getattr(self, 'data', None): return
        row = ['id', 'last_time', 'sign_up_time']
        keys = ['floor', 'satCard', 'bux', 'ChangeBuxPlus', 'ChangeBuxNeg',
                'diamond', 'ChangeDiamondPlus', 'ChangeDiamondNeg', 'dream',
                'friends',
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
                row.append(res.get(k, ''))
            self.csv.writerow(row)
        self.data.clear()

class MailsToGM(Parser):
    cate = 'MailsToGM'
    req = 'cate:Mail sub:purge'
    pattern = re.compile('(?P<date>.*?)\s\[(INFO|WARN)\].*ACTIVITY\splayer:(?P<player>34586) .* cate:Mail sub:purge json:(?P<json>\[.*\])')

    def deal_data(self, res, line):
        jsonstr = res['json']
        purged_items = json.loads(jsonstr)[1]
        for item in purged_items:
            dt = datetime.datetime.fromtimestamp(int(item['date'])/1000).strftime('%Y-%m-%d %H:%M')
            fromId = item['fromId']
            content = item['content']
            self.csv.writerow([dt, fromId, content])

parsers = [SignUp(), IAP(), FirstPurchaseAfterIAP(),
    ScratchCardReward(), Session(), Daily(), MailsToGM()]

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
        if not f.startswith('activity.'): continue
        parts = f.split('.')
        if parts[-1] != 'bz2': continue
        if (last_date != parts[1] or last_region != parts[2]):
            batch_process(batch, last_date, last_region)
            last_date = parts[1]
            last_region = parts[2]
        batch.append(f)
    batch_process(batch, last_date, last_region)

# deal with party.20xxxxxx.region.csv after generated Daily csv

class Party(Parser):
    cate = 'Party'

    def load_relative(self, date, region):
        self.merge_data = {}
        global prefix
        name = prefix + '.'.join(['Daily', date, region, 'csv'])
        # 'tower.Daily.2012-04-12.us-east-1.csv'
        if not os.path.isfile(name):
            self.csv_name = None
            return;
        with open(name, 'rb') as f:
            reader = csv.reader(f)
            for row in reader:
                if not row: continue
                self.merge_data[row[0]] = row[1:]
        self.date = date

    def prepare_data(self):
        self.csv.writerow(
            ['date', 'id', 'found', 'reward', 'participated']
             + self.merge_data['id'])

    def parse(self, row):
        #if self.csv is None: return
        id_ = row[0]
        parties = cassa_get(self.region, id_, 'Parties', '')
        parties = set(parties.split(','))
        participated = 0
        # NOTE: there is ('') in set `parties`
        for p in parties:
            if p < self.date: participated += 1
        parties.add(self.date)
        cassa_insert(self.region, id_, 'Parties', ','.join(parties))
        self.csv.writerow(
            [self.date, id_, row[1], row[3], participated] +
            self.merge_data.get(id_, []))

    def clear_data(self):
        self.merge_data = None

party_parser = Party()
for root, dirs, files in os.walk(log_path):
    while dirs: dirs.pop()
    files.sort()
    while files:
        f = files.pop(0)
        # f == 'party.20120409.ap-northeast-1.ip-10-154-21-236.csv.bz2'
        if not f.startswith('party.'): continue
        parts = f.split('.')
        if parts[-1] != 'bz2': continue
        date = parts[1]
        date = '-'.join([date[:4], date[4:6], date[6:]])
        region = parts[2]
        party_parser.new_csv(date, region)
        if not party_parser.csv: continue
        csv_file = bz2.BZ2File(os.path.join(root, f))
        reader = csv.reader(csv_file)
        for row in reader:
            if row:
                party_parser.parse(row)
        csv_file.close();
        party_parser.close()

