__author__ = 'sunlei'
config = {
    #firehose url
    'url': 'http://firehose0.i.api.weibo.com:8082/comet',
    #firehose appid
    'appid': 'tech_useranalyse',
    #firehose filter
    'filter': 'status,*',
    #firehose loc, None for new connection
    'loc': None,

    #redis client number
    'nrediscli': 1,
    #redis servers: (host, port)
    'redis_servers': [('10.77.109.117', 6667)],
    #redis schema name
    'schema_name': ['vsl']
}