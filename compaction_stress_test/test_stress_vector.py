__author__ = 'sunlei 2014.08.04'

import redis, threading, time, random, stresstest_config


#list of schema {name(string):[(columnname,columnlen)]}
schemas = dict()
#each item is [(columnname(string), columnlen(number))]
columns = list()
#{key: [schema]}
keys = dict()
#it's a set
id_metas = set()
keylock = threading.RLock()


def get_schema():
    return random.choice(schemas.keys())


def get_id_metaid():
    return random.randrange(stresstest_config.id_metarange[0], stresstest_config.id_metarange[0] + stresstest_config.id_metarange[1])


def get_key():
    return random.randrange(stresstest_config.keyrange[0], stresstest_config.keyrange[0] + stresstest_config.keyrange[1])



class read_vector(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.rediscli = redis.Redis(host=stresstest_config.host1, port=stresstest_config.port1, db=0)
        self.__doc__ = 'for vrange vmerge vcount vcard'




class write_vector(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.rediscli = redis.Redis(host=stresstest_config.host1, port=stresstest_config.port1, db=0)
        self.__doc__ = 'for vadd vrem vremrange'

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

        keylock.release()

        id_meta = get_id_metaid()
        if id_meta not in id_metas:
            id_metas.add(id_meta)

        self.rediscli.vadd(str(key)+'.'+schema, get_id_metaid(),
                           *[random.randrange(0, 1 << columns[i][1]) for i in schemas[schema]])

    def run(self):
        for i in range(0, 10):
            self.vadd()



class stress_test_vector(object):
    def __init__(self):
        self.rediscli1 = redis.Redis(host=stresstest_config.host1, port=stresstest_config.port1, db=0)
        self.rediscli2 = redis.Redis(host=stresstest_config.host2, port=stresstest_config.port2, db=0)

    def initschema(self):
        self.initcolumn()

        for i in range(0, stresstest_config.schemarange):
            schema = 's' + str(i)
            self.rediscli1.config_schema('add', schema)
            self.rediscli2.config_schema('add', schema)
            schemas.update({schema: []})

            #add random columns into current schema in order
            for j in range(0, stresstest_config.columnrange):
                schemas[schema].append(j)
                self.rediscli1.config_column('add', schema, columns[j][0], columns[j][1])
                self.rediscli2.config_column('add', schema, columns[j][0], columns[j][1])

    def initcolumn(self):
        for i in range(0, stresstest_config.columnrange):
            columns.append(('c' + str(i), random.choice([1, 2, 4, 8])))

    def run(self):
        readers1 = []
        writers1 = []
        readers2 = []
        writers2 = []
        for i in range(0, stresstest_config.nreadThread):
            readers1.append(read_vector())
            readers2.append(read_vector())
            readers1[i].start()
            readers2[i].start()

        for i in range(0, stresstest_config.nwriteThread):
            writers1.append(write_vector())
            writers2.append(write_vector())
            writers1[i].start()
            writers2[i].start()



stresstest = stress_test_vector()
stresstest.initschema()

mywrite = write_vector()
mywrite.start()


