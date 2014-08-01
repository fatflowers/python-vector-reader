__author__ = 'sunlei'

import redis

rediscli = redis.Redis(host='10.77.109.117', port=6379, db=0)

rediscli.vadd('123.s3', 1406870503888888, 1, 10)
rediscli.vadd('123.s3', 1406870503888889, 1, 10)
rediscli.vadd('123.s3', 1406870503888890, 1, 10)
#rediscli.vcard('123.s3')
rediscli.vrange('123.s3', '123.s3', 0, 1406870503888888, 1406870503888890)

#rediscli.vrem('123.s3', 1406870503888888)
#rediscli.vremrange('123.s3', 1406870503888888, 1406870503888890)
#rediscli.sadd('simon', 'asb', 'asdf')