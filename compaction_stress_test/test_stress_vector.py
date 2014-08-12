__author__ = 'sunlei 2014.08.04'
'''
Functions and classes in this file read config from stresstest_config.py.

stress_test_vector is in the main thread, it configs schemas and columns and creates threads
for class read_vector and write_vector.

read_vector execute vrange vmerge vcount vcard of vector service of redis at a rate.

write_vector execute vadd vrem vremrange at a rate.

'''
import redis
import threading
import random
import stresstest_config
import logging
import time
import pickle
import sys

# list of schema {name(string):[(columnname,columnlen)]}
schemas = dict()
#each item is [(columnname(string), columnlen(number))]
columns = list()
#{key: [schema]}
keys = dict()
#it's a set, avoid duplication
id_metas = set()
#lock keys and id_metas between read and write thread
keylock = threading.RLock()
#log module
logging.basicConfig(filename=stresstest_config.config.log_file, format='%(asctime)s - %(threadName)s- %(funcName)s: %(message)s')
logger = logging.getLogger('stress_test')
logger.setLevel(logging.INFO)


def get_schema():
    return random.choice(schemas.keys())


def get_id_metaid():
    return random.randrange(stresstest_config.config.id_metarange[0],
                            stresstest_config.config.id_metarange[0] + stresstest_config.config.id_metarange[1])


def get_key():
    return random.randrange(stresstest_config.config.keyrange[0],
                            stresstest_config.config.keyrange[0] + stresstest_config.config.keyrange[1])

'''
A thread pickles schemas, columns, keys, id_metas into a file
    used to recover without init the datas
'''
class save_data(threading.Thread):
    def __init__(self, thread_name):
        threading.Thread.__init__(self, name=thread_name)

    def run(self):
        while True:
            time.sleep(stresstest_config.config.backup_interval)
            keylock.acquire()
            try:
                pickle.dump([schemas, columns, keys, id_metas], open(stresstest_config.config.backup_filename, 'wb'))
                logger.info('dump data to ' + stresstest_config.config.backup_filename)
            except pickle.PicklingError:
                logger.debug('unpicklable objects' + time.asctime())

            keylock.release()


class read_vector(threading.Thread):
    def __init__(self, thread_name):
        threading.Thread.__init__(self, name=thread_name)
        self.rediscli1 = redis.Redis(host=stresstest_config.config.host1, port=stresstest_config.config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.config.host2, port=stresstest_config.config.port2, db=0)
        self.__doc__ = 'for vrange vmerge vcount vcard'

    def vrange(self):
        keylock.acquire()

        #avoid nothing to remove
        if len(keys) == 0 or len(id_metas) == 0:
            keylock.release()
            return

        key = random.choice(list(keys))
        key = str(key) + '.' + str(random.choice((keys[key])))
        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            keylock.release()
            return

        keylock.release()
        try:
            filter_ = 0
            self.rediscli1.vrange(key, key, filter_, start_id, stop_id)
            self.rediscli2.vrange(key, key, filter_, start_id, stop_id)
            if stresstest_config.config.enable_log:
                logger.info('%s %s %s %s %s', *(key, key, str(filter_), str(start_id), str(stop_id)))
        except Exception as err:
            logger.debug(err)

    def vmerge(self):
        keylock.acquire()

        #avoid nothing to remove
        if len(keys) == 0 or len(id_metas) == 0:
            keylock.release()
            return

        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            keylock.release()
            return
        key = set()
        #select random keys to merge
        for i in range(0, random.randrange(1, len(keys))):
            tmpkey = random.choice(list(keys))
            tmpkey = str(tmpkey) + '.' + str(random.choice((keys[tmpkey])))
            key.add(tmpkey)
        keylock.release()
        try:
            filter_ = 0
            arg = list(key)
            arg.extend([filter_, 100, start_id, stop_id])
            self.rediscli1.vmerge(*arg)
            self.rediscli2.vmerge(*arg)
            if stresstest_config.config.enable_log:
                logger.info(str(arg)[1:-1].replace(',', ' '))
        except Exception as err:
            logger.debug(err)

    def vcount(self):
        keylock.acquire()

        #avoid nothing to remove
        if len(keys) == 0 or len(id_metas) == 0:
            keylock.release()
            return

        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            keylock.release()
            return
        key = random.choice(list(keys))
        key = str(key) + '.' + str(random.choice((keys[key])))

        keylock.release()
        try:
            self.rediscli1.vcount(key, start_id, stop_id)
            self.rediscli2.vcount(key, start_id, stop_id)
            if stresstest_config.config.enable_log:
                logger.info('%s %s %s', *(key, str(start_id), str(stop_id)))
        except Exception as err:
            logger.debug(err)

    def vcard(self):
        keylock.acquire()

        #avoid nothing to remove
        if len(keys) == 0 or len(id_metas) == 0:
            keylock.release()
            return

        key = random.choice(list(keys))
        key = str(key) + '.' + str(random.choice((keys[key])))

        keylock.release()
        try:
            self.rediscli1.vcard(key)
            self.rediscli2.vcard(key)
            if stresstest_config.config.enable_log:
                logger.info(key)
        except Exception as err:
            logger.debug(err)

    def run(self):
        #execution rate:(vrange+vmerge)/(vcount+vcard) = 10/1
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
    def __init__(self, thread_name):
        threading.Thread.__init__(self, name=thread_name)
        self.rediscli1 = redis.Redis(host=stresstest_config.config.host1, port=stresstest_config.config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.config.host2, port=stresstest_config.config.port2, db=0)
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
            self.rediscli1.vadd(str(key) + '.' + schema, id_meta_id, *cols)
            self.rediscli2.vadd(str(key) + '.' + schema, id_meta_id, *cols)
            if stresstest_config.config.enable_log:
                logger.info('%s.%s %s %s', *(str(key), str(schema), str(id_meta_id), str(cols)[1:-1].replace(',', ' ')))
        except Exception as err:
            logger.debug(err)

    def vrem(self):
        keylock.acquire()
        #avoid nothing to remove
        if len(keys) == 0 or len(id_metas) == 0:
            keylock.release()
            return

        key = random.choice(list(keys))
        key = str(key) + '.' + str(random.choice((keys[key])))
        ids = set()
        for i in range(0, random.choice([1, 2])):
            ids.add(random.choice(list(id_metas)))
        ids = list(ids)
        keylock.release()
        try:
            self.rediscli1.vrem(key, *ids)
            self.rediscli2.vrem(key, *ids)
            if stresstest_config.config.enable_log:
                logger.info(key + ' %s', str(ids)[1:-1].replace(',', ' '))
        except Exception as err:
            logger.debug(err)

    def vremrange(self):
        keylock.acquire()
        #avoid nothing to remove
        if len(keys) == 0 or len(id_metas) == 0:
            keylock.release()
            return

        key = random.choice(list(keys))
        key = str(key) + '.' + str(random.choice((keys[key])))
        start_id = random.choice(list(id_metas))
        stop_id = random.choice(list(id_metas))
        if start_id >= stop_id:
            keylock.release()
            return

        keylock.release()

        try:
            self.rediscli1.vremrange(key, start_id, stop_id)
            self.rediscli2.vremrange(key, start_id, stop_id)
            if stresstest_config.config.enable_log:
                logger.info(key + ' %s %s', str(start_id), str(stop_id))
        except Exception as err:
            logger.debug(err)

    def run(self):
        #command execution rate: add/rem = 10/1
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
        self.rediscli1 = redis.Redis(host=stresstest_config.config.host1, port=stresstest_config.config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.config.host2, port=stresstest_config.config.port2, db=0)

    def __del__(self):
        logging.shutdown()

    def initschema(self):
        self.initcolumn()
        global schemas
        for i in range(0, stresstest_config.config.schemarange):
            schema = 's' + str(i)
            try:
                self.rediscli1.config_schema('add', schema)
                self.rediscli2.config_schema('add', schema)
                schemas.update({schema: []})
                if stresstest_config.config.enable_log:
                    logger.info('config schema add %s', str(schema))
            except Exception as err:
                logger.debug(err)

            #add columns into current schema in order
            for j in range(0, stresstest_config.config.columnrange):
                schemas[schema].append(j)
                try:
                    self.rediscli1.config_column('add', schema, columns[j][0], columns[j][1])
                    self.rediscli2.config_column('add', schema, columns[j][0], columns[j][1])
                    if stresstest_config.config.enable_log:
                        logger.info('config column add %s %s %s', *(str(schema), columns[j][0], str(columns[j][1])))
                except Exception as err:
                    logger.debug(err)

    def initcolumn(self):
        global columns
        for i in range(0, stresstest_config.config.columnrange):
            if stresstest_config.config.columnrange != 4:
                columns.append(('c' + str(i), random.choice([1, 2, 4, 8])))
            else:
                columns.append(('c' + str(i), [1, 2, 4, 8][i]))

    def run(self):
        #if start with backup load datas from file,
        #  won't init schemas and columns
        if stresstest_config.config.start_with_backup or len(sys.argv) == 2:
            try:
                global schemas, columns, keys, id_metas
                [schemas, columns, keys, id_metas] = pickle.load(open(stresstest_config.config.backup_filename, 'rb'))
                logger.info('load data from ' + stresstest_config.config.backup_filename)
            except Exception:
                logger.debug('unpickling error')
        else:
            self.initschema()

        #start the backup thread
        backup_thread = save_data('backup_thread')
        backup_thread.start()

        writers = []
        readers = []

        for i in range(0, stresstest_config.config.nwriteThread):
            writers.append(write_vector('write_thread_' + str(i)))
            writers[i].start()

        #sleep for some time, avoid nothing to read
        time.sleep(stresstest_config.config.sleep_before_reading)

        for i in range(0, stresstest_config.config.nreadThread):
            readers.append(read_vector('read_thread_' + str(i)))
            readers[i].start()


stresstest = stress_test_vector()
stresstest.run()



