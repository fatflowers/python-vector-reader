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
        self.url = config['url']
        self.header = {
            'appid': config['appid'],
            'fileter': config['filter']
        }
        if config['loc'] is not None:
            self.header['loc'] = config['loc']

        # init logger
        logging.basicConfig(filename='import.log',
                            format='%(asctime)s - %(threadName)s- %(funcName)s: %(message)s')
        self.logger = logging.getLogger('import_log')
        self.logger.setLevel(logging.INFO)

        # current location of firehose
        self.loc = -1

        # redis client
        self.rediscli = [redis.Redis(host=config['redis_servers'][i][0], port=config['redis_servers'][i][1], db=0) for i
                         in
                         range(0, config['nrediscli'])]

    # parse json object and send redis command
    def parse_send(self, json_obj):
        pass

    def connect(self):
        # assemble header
        prepped = requests.Request('GET', self.url).prepare()
        prepped.headers = self.header
        if self.loc != -1:
            prepped.headers['loc'] = self.loc
        # session, default keep-alive
        session = requests.Session()
        # response of 'GET' request
        resp = session.send(prepped, stream=True)
        try:
            if resp.status_code == requests.codes.ok:
                self.logger.info('start import')
                #keep reading
                for line in resp.iter_lines():
                    if line:
                        json_obj = json.loads(line)
                        # refresh firehose location parameter
                        self.loc = json_obj['id']
                        self.parse_send(json_obj)
        except:
            pass

    def run(self):
        while True:
            self.connect()
