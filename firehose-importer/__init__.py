__author__ = 'sunlei'
config = {
    #firehose url
    'url': 'http://firehose0.i.api.weibo.com:8082/comet',
    #firehose appid
    'appid': 'tech_useranalyse',
    #firehose filter
    'filter': 'status,*',
    #firehose loc
    'loc': None,

    #redis client number
    'nrediscli': 2,
    #redis servers: (host, port)
    #number of servers should be equal to nrediscli
    'redis_servers': [('10.77.109.117',6379), ('10.77.109.117', 6666)]
}