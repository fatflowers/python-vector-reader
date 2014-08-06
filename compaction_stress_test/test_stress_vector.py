__author__ = 'sunlei 2014.08.04'

import redis
import threading
import random
import stresstest_config
import logging

#list of schema {name(string):[(columnname,columnlen)]}
schemas = dict()
#each item is [(columnname(string), columnlen(number))]
columns = list()
#{key: [schema]}
keys = dict()
#it's a set
id_metas = set()
keylock = threading.RLock()
#log module
logging.basicConfig(filename='log.txt', format='%(asctime)s - %(funcName)s: %(message)s')
logger = logging.getLogger('stress_test')
logger.setLevel(logging.INFO)


def get_schema():
    return random.choice(schemas.keys())


def get_id_metaid():
    return random.randrange(stresstest_config.id_metarange[0], stresstest_config.id_metarange[0] + stresstest_config.id_metarange[1])


def get_key():
    return random.randrange(stresstest_config.keyrange[0], stresstest_config.keyrange[0] + stresstest_config.keyrange[1])



class read_vector(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.rediscli1 = redis.Redis(host=stresstest_config.host1, port=stresstest_config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.host2, port=stresstest_config.port2, db=0)
        self.__doc__ = 'for vrange vmerge vcount vcard'

    def vrange(self):
        keylock.acquire()

        key = random.choice(list(keys))
        key = str(key) + '.' + str(keys[key])
        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            return

        keylock.release()
        try:
            filter_ = 0
            self.rediscli1.vrange(key, key, filter_, start_id, stop_id)
            self.rediscli2.vrange(key, key, filter_, start_id, stop_id)
            logger.info(key + ' ' + key + ' ' + str(filter_) + ' %s %s', str(start_id), str(stop_id))
        except Exception as err:
            logger.debug(err)

    def vmerge(self):
        keylock.acquire()

        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            return
        key = set()
        #select random keys to merge
        for i in range(0, random.randrange(1, len(keys))):
            tmpkey = random.choice(list(keys))
            tmpkey = str(tmpkey) + '.' + str(keys[tmpkey])
            key.add(tmpkey)
        keylock.release()
        try:
            filter_ = 0
            arg = list(key)
            arg.extend([filter_, 100, start_id, stop_id])
            self.rediscli1.vmerge(*arg)
            self.rediscli2.vmerge(*arg)
            logger.info('%s %s %s %s %s', str(arg), str(filter), str(100), str(start_id), str(stop_id))
        except Exception as err:
            logger.debug(err)

    def vcount(self):
        pass

    def vcard(self):
        pass

    def run(self):
        vrangerate = 10.0 / 22.0
        vmergerate = 20.0 / 22.0
        vcountrate = 21.0 / 22.0
        while True:
            rand = random.random()
            if rand < vrangerate:
                self.vrange()
            elif rand < vmergerate:
                self.vmerge()
            elif rand < vcountrate:
                self.vcount()
            else:
                self.vcard()

class write_vector(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.rediscli1 = redis.Redis(host=stresstest_config.host1, port=stresstest_config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.host2, port=stresstest_config.port2, db=0)
        self.__doc__ = 'for vadd vrem vremrange'
        self.functionNumber = 1

    def vadd(self):
        key = get_key()
        schema = get_schema()

        keylock.acquire()

        if key in keys:
            if schema not in keys[key]:
                keys[key].append(schema)
            else:
                pass
        else:
            keys[key] = [schema]

        id_meta = get_id_metaid()
        if id_meta not in id_metas:
            id_metas.add(id_meta)

        keylock.release()

        try:
            id_meta_id = get_id_metaid()
            cols = [random.randrange(0, 1 << columns[i][1]) for i in schemas[schema]]
            self.rediscli1.vadd(str(key)+'.'+schema, id_meta_id, *cols)
            self.rediscli2.vadd(str(key)+'.'+schema, id_meta_id, *cols)
            logger.info('%s.%s %s %s', *(str(key), str(schema), str(id_meta_id), str(cols)))
        except Exception as err:
            logger.debug(err)

    def vrem(self):
        keylock.acquire()

        key = random.choice(list(keys))
        key = str(key) + '.' + str(keys[key])
        ids = set()
        for i in range(0, len(id_metas)):
            ids.add(random.choice(list(id_metas)))

        keylock.release()
        try:
            self.rediscli1.vrem(key, *ids)
            self.rediscli2.vrem(key, *ids)
            logger.info(key + ' %s', str(ids))
        except Exception as err:
            logger.debug(err)

    def vremrange(self):
        keylock.acquire()

        key = random.choice(list(keys))
        key = str(key) + '.' + str(keys[key])
        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            return

        keylock.release()

        try:
            self.rediscli1.vremrange(key, start_id, stop_id)
            self.rediscli2.vremrange(key, start_id, stop_id)
            logger.info(key + ' %s %s', str(start_id), str(stop_id))
        except Exception as err:
            logger.debug(err)

    def run(self):
        #add/rem = 10/1
        vaddrate = 20.0 / 22.0
        vremrate = 21.0 / 22.0
        while True:
            rand = random.random()
            if rand < vaddrate:
                self.vadd()
            elif rand < vremrate:
                self.vrem()
            else:
                self.vremrange()


class stress_test_vector(object):
    def __init__(self):
        self.rediscli1 = redis.Redis(host=stresstest_config.host1, port=stresstest_config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.host2, port=stresstest_config.port2, db=0)

    def initschema(self):
        self.initcolumn()

        for i in range(0, stresstest_config.schemarange):
            schema = 's' + str(i)
            try:
                self.rediscli1.config_schema('add', schema)
                self.rediscli2.config_schema('add', schema)
                schemas.update({schema: []})
                logger.info('config schema add %s', str(schema))
            except Exception as err:
                logger.debug(err)

            #add columns into current schema in order
            for j in range(0, stresstest_config.columnrange):
                schemas[schema].append(j)
                try:
                    self.rediscli1.config_column('add', schema, columns[j][0], columns[j][1])
                    self.rediscli2.config_column('add', schema, columns[j][0], columns[j][1])
                    logger.info('config column add %s %s %s', *(str(schema), columns[j][0], str(columns[j][1])))
                except Exception as err:
                    logger.debug(err)

    def initcolumn(self):
        for i in range(0, stresstest_config.columnrange):
            if stresstest_config.columnrange != 4:
                columns.append(('c' + str(i), random.choice([1, 2, 4, 8])))
            else:
                columns.append(('c' + str(i), [1, 2, 4, 8][i]))

    def run(self):
        readers = []
        writers = []
        for i in range(0, stresstest_config.nreadThread):
            readers.append(read_vector())
            readers[i].start()

        for i in range(0, stresstest_config.nwriteThread):
            writers.append(write_vector())
            writers[i].start()



stresstest = stress_test_vector()
stresstest.initschema()
stresstest.run()



