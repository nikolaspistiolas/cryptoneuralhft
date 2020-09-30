import pymongo
import copy

# The data are expected in the initial collection in the format {'_id':timestamp,
#                                           'orderbook':['asks':[{'size':size,'price':price},{}],
#                                           'bids':[{'size':size,'price':price},{}]]}
class DataPreprocessing():
    def __init__(self,db,initial_col,depth):
        client = pymongo.MongoClient('localhost',27017)
        self.db = client[db]
        if depth > 20:
            print("too much depth")
            raise Exception
        self.depth = depth
        self.initial_col = initial_col

    def check_same(self,data):
        if data['asks'][0]['size'] == data['bids'][0]['size'] and data['asks'][0]['price'] == data['bids'][0]['price']:
            return False
        else:
            return True

    def keep_depth(self):
        col1 = self.db[self.initial_col]
        col2 = self.db['depth'+str(self.depth)]
        data = col1.find()
        for i in data:
            id = i['_id']
            asks = []
            bids = []
            try:
                orderbook = i['orderbook']
            except:
                print('EXCEPTION',i)
                continue
            for ask in orderbook['asks']:
                asks.append({'price': float(ask['price']), 'size': float(ask['size'])})
            for bid in orderbook['bids']:
                bids.append({'price': float(bid['price']), 'size': float(bid['size'])})
            up = {'_id': id,
                  'asks': asks[:self.depth],
                  'bids': bids[:self.depth]}
            if self.check_same(up):
                col2.insert_one(up)
        return

    def mean_sd(self):
        c = 0
        col = self.db['depth' + str(self.depth)]
        col2 = self.db['meansd']
        col2.delete_many({})
        data = col.find()
        work = []
        for i in data:
            work.append(i)
            if len(work) == 48*60:
                break
        sumx_price = 0
        sumx_size = 0
        sumx2_price = 0
        sumx2_size = 0
        count = 0
        for i in work:
            for j in i['asks']:
                count += 1
                sumx_price += j['price']
                sumx2_price += j['price']**2
                sumx_size += j['size']
                sumx2_size += j['size']**2
            for j in i['bids']:
                count += 1
                sumx_price += j['price']
                sumx2_price += j['price']**2
                sumx_size += j['size']
                sumx2_size += j['size']**2
        for i in data:
            c += 1
            for j in work[0]['asks']:
                count -= 1
                sumx_price -= j['price']
                sumx2_price -= j['price']**2
                sumx_size -= j['size']
                sumx2_size -= j['size']**2
            for j in work[0]['bids']:
                count -= 1
                sumx_price -= j['price']
                sumx2_price -= j['price']**2
                sumx_size -= j['size']
                sumx2_size -= j['size']**2

            for j in i['asks']:
                count += 1
                sumx_price += j['price']
                sumx2_price += j['price']**2
                sumx_size += j['size']
                sumx2_size += j['size']**2
            for j in i['bids']:
                count += 1
                sumx_price += j['price']
                sumx2_price += j['price']**2
                sumx_size += j['size']
                sumx2_size += j['size']**2
            work.append(i)
            work = work[1:]
            meanprice = sumx_price/count
            meansize = sumx_size/count
            sdprice = ( sumx2_price/count - meanprice**2 ) * (count/ (count-1) )
            sdprice = sdprice **0.5
            sdsize = (sumx2_size/count - meansize**2) * (count/ (count-1))
            sdsize = sdsize**0.5
            up = copy.deepcopy(i)
            up['stats'] = {'mean_price':meanprice,'mean_size':meansize,'sd_price':sdprice,'sd_size':sdsize}
            col2.insert_one(up)
            if c % 10000 == 0:
                print(c)

    def normalize(self):
        c = 0
        col = self.db['meansd']
        col2 = self.db['normalize']
        col2.delete_many({})
        data = col.find()
        for i in data:
            c +=1
            for j in i['asks']:
                j['price'] = (j['price'] - i['stats']['mean_price'])/i['stats']['sd_price']
                j['size'] = (j['size'] - i['stats']['mean_size']) / i['stats']['sd_size']
            for j in i['bids']:
                j['price'] = (j['price'] - i['stats']['mean_price'])/i['stats']['sd_price']
                j['size'] = (j['size'] - i['stats']['mean_size']) / i['stats']['sd_size']
            col2.insert_one(i)
            if c % 10000 == 0:
                print(c)

    def create_midprice(self):
        col = self.db['normalize']
        col2 = self.db['midprices']
        data = col.find()
        col2.delete_many({})
        c = 0
        for i in data:
            c += 1
            midprice = (i['asks'][0]['price'] + i['bids'][0]['price'])/2
            u = copy.deepcopy(i)
            u['midprice'] = midprice
            col2.insert_one(u)
            if c % 10000 == 0:
                print(c)
        return

    def create_percentages(self):
        col = self.db['midprices']
        col2 = self.db['percentages']
        col2.delete_many({})
        data = col.find()
        c = 0
        work = []
        work.append(data.next())
        work.append(data.next())
        work.append(data.next())
        work.append(data.next())
        for i in data:
            work.append(i)
            c += 1
            i['per1'] = (work[0]['midprice'] - work[1]['midprice'])/work[0]['midprice']
            i['per2'] = (work[0]['midprice'] - work[2]['midprice']) / work[0]['midprice']
            i['per3'] = (work[0]['midprice'] - work[3]['midprice']) / work[0]['midprice']
            i['per4'] = (work[0]['midprice'] - work[4]['midprice']) / work[0]['midprice']
            if c % 10000 == 0 : print(c)
            col2.insert_one(i)
            work = work[1:]
        return

    def fill_zeros(self):
        col = self.db['percentages']
        col2 = self.db['zero_filled']
        data = col.find()
        col2.delete_many({})
        prevtime = col.find_one()['_id']
        for i in data:
            time = i['_id']
            t = time-prevtime-30
            t = float(t)/60.0
            t = int(t)

            up = copy.deepcopy(i)
            up['is_zero'] = False
            col2.insert_one(up)
            if t > 0:
                for j in range(t):
                    prevtime += 60
                    try:
                        col2.insert_one({'_id':prevtime,'is_zero':True})
                    except:
                        pass
            prevtime = time

    def create_entries(self):
        col = self.db['zero_filled']
        col2 = self.db['entries']
        data = col.find()
        col2.delete_many({})
        work = []
        c = 0
        for _ in range(100):
            work.append(data.next())
        for i in data:
            c += 1
            if c % 10000 == 0:
                print(c)
            zeros = 0
            work = work[1:]
            work.append(i)
            inp = []
            for j in work:
                if j['is_zero'] == True:
                    zeros += 1
                    inp.append([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
                else:
                    bids = []
                    for b in j['bids']:
                        bids.append(b['price'])
                        bids.append(b['size'])
                    asks = []
                    for a in j['asks']:
                        asks = [a['price']] + asks
                        asks = [a['size']] + asks
                    inp.append(asks + bids)
            up = copy.deepcopy(i)
            up['input'] = inp
            if zeros < 8:
                col2.insert_one(up)
        return

    def split(self):
        col = self.db['entries']
        train = self.db['train']
        val = self.db['val']
        test = self.db['test']
        train.delete_many()
        val.delete_many()
        test.delete_many()
        count = col.count()
        data = col.find()
        counter = 0
        for i in data:
            counter += 1
            if counter < 0.8*count:
                train.insert_one(i)
            elif counter < 0.9*count:
                val.insert_one(i)
            else:
                test.insert_one(i)
            if counter % 10000 == 0:
                print(counter)

# a = DataPreprocessing('shrimpy_hitbtc_eth_btc','data',10)
# print(1)
# a.keep_depth()
# print(2)
# a.mean_sd()
# print(3)
# a.normalize()
# print(4)
# a.create_midprice()
# print(5)
# a.create_percentages()
# print(6)
# a.fill_zeros()
# print(7)
# a.create_entries()
#
# a.split()
