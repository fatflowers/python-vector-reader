__author__ = 'sunlei 2014.08.12'

import json
import requests
import logging
import threading
import redis

from __init__ import config


class Importer(threading.Thread):
    def __init__(self, thread_name='importer'):
        self.__doc__ = 'Import data from firehose and send it to redis'
        # for multi-thread
        threading.Thread.__init__(self, name=thread_name)
        # init url and header
        self.url = config['url'] + '?appid=' + config['appid'] + '&filter=' + config['filter']

        # current location of firehose
        if config['loc'] is not None:
            self.loc = config['loc']
        else:
            self.loc = -1

        # init logger
        logging.basicConfig(filename='import.log',
                            format='%(asctime)s - %(threadName)s- %(funcName)s: %(message)s')
        self.logger = logging.getLogger('import_log')
        self.logger.setLevel(logging.INFO)



        # redis client
        self.rediscli = [redis.Redis(host=config['redis_servers'][i][0], port=config['redis_servers'][i][1], db=0) for i
                         in
                         range(0, len(config['redis_servers']))]

        # for debug keep-alive connection after 10mins
        self.printd = 0

    def __del__(self):
        pass

    # parse json object and send redis command
    def parse_send(self, json_obj):
        value = 0

        if self.loc != -1 and self.printd < 5:
            self.logger.info(
                str(json_obj['id']) + 'created at' + json_obj['text']['status']['created_at'] + ' and self.loc:' + str(
                    self.loc))
            self.printd += 1
        # execute redis commands for weibo
        if json_obj['text']['type'] == 'status':
            for cli in self.rediscli:
                if json_obj['text']['event'] == 'add' or json_obj['text']['event'] == 'update':
                    cli.vadd(*[
                        str(json_obj['text']['status']['user']['id']) + '.' + config['schema_name'][0],
                        json_obj['text']['status']['mid'],
                        value
                    ])

                elif json_obj['text']['event'] == 'delete':
                    cli.vrem(*[
                        str(json_obj['text']['status']['user']['id']) + '.' + config['schema_name'][0],
                        json_obj['text']['status']['mid']
                    ])

    # noinspection PyBroadException
    def connect(self):
        # add loc to url
        if self.loc != -1:
            self.logger.info('start with loc' + str(self.loc))
            prepped = requests.Request('GET', self.url + '&loc=' + str(self.loc)).prepare()
        else:
            prepped = requests.Request('GET', self.url).prepare()

        # session, default keep-alive
        session = requests.Session()
        # response of 'GET' request
        resp = session.send(prepped, stream=True)

        #for debug
        # config schema add vsl
        # config column add vsl c1 8
        self.printd = 0
        try:
            if resp.status_code == requests.codes.ok:
                self.logger.info('start import')
                # keep reading
                for line in resp.iter_lines():
                    if line:
                        json_obj = json.loads(line)
                        # refresh firehose location parameter
                        self.loc = json_obj['id']
                        self.parse_send(json_obj)
        except:
            self.logger.info("error!")

    def run(self):
        while True:
            self.connect()


im = Importer()
im.start()
