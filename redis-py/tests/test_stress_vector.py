__author__ = 'sunlei 2014.08.04'

import redis, threading, time, random, test_stress_vector_global

class read_vector(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.rediscli = redis.Redis(host=test_stress_vector_global.host, port=test_stress_vector_global.port, db=0)
    def vadd(self):
        pass
class write_vector(threading.Thread):
    pass

class stress_test_vector(object):
    def __init__(self, keyrange, id_metarange, schemarange,
                 columnrange, nreadThread, nwriteThread, in_host, in_port):
        #keyrange and id_metarange should be tuples:(start_id, offset)
        test_stress_vector_global.keyrange = keyrange
        test_stress_vector_global.id_metarange = id_metarange

        #schemarange and columnrange are max numbers of schema and column
        test_stress_vector_global.schemarange = schemarange
        test_stress_vector_global.columnrange = columnrange

        test_stress_vector_global.nreadThread = nreadThread
        test_stress_vector_global.nwriteThread = nwriteThread

        test_stress_vector_global.host = in_host
        test_stress_vector_global.port = in_port

        self.rediscli = redis.Redis(host=in_host, port=in_port, db=0)

    #initialize schema and columns
    def initschema(self):
        self.initcolumn()

        for i in range(0, test_stress_vector_global.schemarange):
            schema = 's' + str(i)
            self.rediscli.config_schema('add', schema)
            test_stress_vector_global.schemas.update({schema: []})


            columnset = set()
            #add random columns into current schema in order
            for j in range(1, random.randrange(0, test_stress_vector_global.columnrange)):
                columnindex = random.randrange(0, test_stress_vector_global.columnrange)
                if columnset.issuperset([columnindex]):
                    continue
                else:
                    columnset.add(columnindex)
                test_stress_vector_global.schemas[schema].append\
                    (test_stress_vector_global.columns[columnindex])
                self.rediscli.config_column('add', schema, test_stress_vector_global.columns[columnindex][0],
                                            test_stress_vector_global.columns[columnindex][1])


    def initcolumn(self):
        for i in range(0, test_stress_vector_global.columnrange):
            test_stress_vector_global.columns.append(('c' + str(i), random.choice([1, 2, 4, 8])))

stresstest = stress_test_vector((0, 2000000), (1406870503888888 ,2000000), 1000, 100, 5, 2, '10.77.109.117', 6379)
stresstest.initschema()




