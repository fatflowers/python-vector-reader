__author__ = 'sunlei'
__doc__ = 'to use this code, you need to download requests <http://docs.python-requests.org>'
config = {
    #firehose url
    'url': 'http://firehose0.i.api.weibo.com:8082/comet',
    #firehose appid
    'appid': 'tech_useranalyse',
    #firehose filter
    'filter': 'status,*',
    #firehose loc, None for new connection
    'loc': None,

    #white list, set of uids needed
    'white_list_enable': True,
    #white list, a set
    'white_list': {()},

    #redis client number
    'nrediscli': 1,
    #redis servers: (host, port)
    'redis_servers': [('10.77.109.117', 6667)],
    #redis schema name
    'schema_name': ['vsl']
}