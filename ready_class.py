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
            asks = bids = []
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

    def keep_work(self,work,time):
        ret = []
        for i in work:
            if i['_id']<= time and i['_id'] >= time - 24*60*60:
                ret.append(i)
        return ret

    def minmaxmean(self):
        c = 0
        col = self.db['depth' + str(self.depth)]
        col2 = self.db['minmax']
        col2.delete_many({})
        first_time = col.find_one()['_id']
        data = col.find()
        work = []
        for i in data:
            work.append(i)
            if first_time + 24 * 60 * 60 > i['_id']:
                break


        min_price = 10000000
        max_price = 0
        min_size = 10000000
        max_size = 0
        for d in data:
            c += 1
            work.append(d)
            work = self.keep_work(work, d['_id'])
            sum_size = 0
            sum_price = 0
            count = 0
            for i in work:
                for j in i['asks']:
                    sum_price += j['price']
                    sum_size += j['size']
                    count += 1
                    if j['size'] < min_size:
                        min_size = j['size']
                    if j['size'] > max_size:
                        max_size = j['size']
                    if j['price'] < min_price:
                        min_price = j['price']
                    if j['price'] > max_price:
                        max_price = j['price']
                for j in i['bids']:
                    sum_price += j['price']
                    sum_size += j['size']
                    count += 1
                    if j['size'] < min_size:
                        min_size = j['size']
                    if j['size'] > max_size:
                        max_size = j['size']
                    if j['price'] < min_price:
                        min_price = j['price']
                    if j['price'] > max_price:
                        max_price = j['price']
                if max_price == min_price:
                    print('price oops')
                if max_size == min_size:
                    print('size oops')
            up = copy.deepcopy(d)
            up['minmax'] = {'minprice':min_price,'maxprice':max_price,'meanprice':sum_price/count,'minsize':min_size,'maxsize':max_size,'meansize':sum_size/count}
            col2.insert_one(up)
            if c % 10000 == 0 :
                print(c)
        return

    def normfortime(self,time_in_min):
        c = 0
        col = self.db['minmax']
        col2 = self.db['normalized_depth'+str(self.depth)]
        col2.delete_many({})
        first_time = col.find_one()['_id']
        data = col.find()
        for j in data:
            min_price = j['minmax']['minprice']
            max_price = j['minmax']['maxprice']
            min_size = j['minmax']['minsize']
            max_size = j['minmax']['maxsize']
            mean_price = j['minmax']['meanprice']
            mean_size = j['minmax']['meansize']
            up = copy.deepcopy(j)
            try:
                for i in up['asks']:

                    i['price'] = (i['price'] - mean_price) / (max_price - min_price)
                    i['size'] = (i['size'] - mean_size) / (max_size - min_size)

                for i in up['bids']:
                    i['price'] = (i['price'] - mean_price) / (max_price - min_price)
                    i['size'] = (i['size'] - mean_size) / (max_size - min_size)
                col2.insert_one(up)
            except:
                print(max_price,max_size,min_price,min_size,up['_id'])
            if c % 10000 == 0:
                print(c)

    def create_midprice(self):
        col = self.db['normalized_depth20']
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

    def mean(self,x:list,means:list):
        counter = 0
        ret = []
        sum = 0
        for i in x:
            counter += 1
            sum += i['midprice']
            if counter in means:
                ret.append(sum/counter)
        return ret

    def smooth_midprices(self):
        col = self.db['midprices']
        col2 = self.db['smooth_midprices']
        data = col.find()
        col2.delete_many({})
        work = []
        counter = 0
        for i in data:
            work.append(i)
            counter +=1
            if counter == 100:
                break
        meanlist = [1, 2, 5, 10]
        for i in data:
            counter +=1
            work = work[1:]
            work.append(i)
            means = self.mean(work,meanlist)
            up = copy.deepcopy(work[0])
            up['mid1'] = means[0]
            up['mid2']= means[1]
            up['mid5']= means[2]
            up['mid10']= means[3]
            col2.insert_one(up)
            if counter % 10000 == 0:
                print(counter)
        return

    def create_percentages(self):
        col = self.db['smooth_midprices']
        col2 = self.db['percentages']
        col2.delete_many({})
        data = col.find()
        counter = 0
        counter2 = 0
        for i in data:
            counter += 1
            u = copy.deepcopy(i)
            try:
                u['per10'] = (u['mid10'] - u['midprice'])/u['mid10']
                u['per20'] = (u['mid20'] - u['midprice'])/u['mid20']
                u['per50'] = (u['mid50'] - u['midprice'])/u['mid50']
                u['per100'] = (u['mid100'] - u['midprice'])/u['mid100']
            except ZeroDivisionError:
                print(u)
                counter2 += 1
                if counter2 % 100 == 0:
                    print('eerr', counter2)

            if counter % 10000 == 0 : print(counter)
            col2.insert_one(u)
        return

    def fill_entries(self):
        col = self.db['percentages']
        col2 = self.db['zero_filled']
        data = col.find()
        prevtime = col.find_one()['_id']
        for i in data:
            time = i['_id']
            t = time-prevtime-30
            t = float(t)/60.0
            t = int(t)
            prevtime = time
            if t > 0:
                for j in range(t):
                    time += 60
                    col2.insert_one({'_id':time,'is_zero':True})
            up = copy.deepcopy(i)
            up['is_zero']=False
            col2.insert_one(up)

    def create_entries(self):
        col = self.db['zero_filled']
        col2 = self.db['entries']
        data = col.find()
        work = []
        for _ in range(110):
            work.append(data.next())
        for i in data:
            work = work[1:]
            work.append(i)
            inp = []
            for j in work:
                bids = []
                for b in j['bids']:
                    bids.append(b['price'])
                    bids.append(b['size'])
                asks = []
                for a in j['asks']:
                    asks = asks + a['price']
                    asks = asks + a['size']
                inp.append(asks + bids)
            up = copy.deepcopy(i)
            up['input'] = inp
            col2.insert_one(up)
        return







