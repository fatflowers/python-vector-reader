__author__ = 'sunlei'

import redis

rediscli = redis.Redis(host='10.77.109.117', port=6379, db=0)

#idmeta_id should be bigger than refresh vector minid
idmeta_id = 1406870503888888


def test_vadd():
    assert rediscli.vadd('123.s3', idmeta_id, 1, 9) == 1
    assert rediscli.vadd('123.s3', idmeta_id + 1, 1, 10) == 1
    assert rediscli.vadd('123.s3', idmeta_id + 2, 1, 11) == 1

    assert rediscli.vadd('123.s4', idmeta_id, 1, 19) == 1
    assert rediscli.vadd('123.s4', idmeta_id + 1, 1, 10) == 1
    assert rediscli.vadd('123.s4', idmeta_id + 3, 1, 11) == 1
    assert rediscli.vadd('123.s4', idmeta_id + 4, 1, 12) == 1

def test_vcard():
    assert rediscli.vcard('123.s3') == 3
    assert rediscli.vcard('123.s4') == 4

def test_vrem():
    assert rediscli.vrem('123.s4', idmeta_id) == 1

def test_vremrange():
    assert rediscli.vremrange('123.s3', idmeta_id, idmeta_id + 3) == 3


def test_vrange():
    rangeresult = rediscli.vrange('123.s3', '123.s3', 0, idmeta_id, idmeta_id + 2)
    for item in rangeresult:
        print(item)

def test_vcount():
    assert rediscli.vcount('123.s3', idmeta_id, idmeta_id + 2) == 3
    assert rediscli.vcount('123.s3', idmeta_id, idmeta_id + 1) == 2

    assert rediscli.vcount('123.s4', idmeta_id, idmeta_id + 4) == 4
    assert rediscli.vcount('123.s4', idmeta_id + 4, idmeta_id + 1) == 0

def test_vmerge():
    mergeresult = rediscli.vmerge('123.s3', '123.s4', 0, 100, 0, idmeta_id + 4)
    #mergeresult [1406870503888888L, 1L, 9L, 1406870503888890L, 1L, 11L, 1406870503888889L, 1L, 10L]
    assert len(mergeresult) == 9

    mergeresult = rediscli.vmerge('123.s3', '123.s4', 0, 100, idmeta_id + 2, idmeta_id + 4)
    assert len(mergeresult) == 3

def test_config_schema():
    #commands to test: add del get show addfilter delfilter
    assert rediscli.config_schema('add', 's3') == 'OK'
    assert rediscli.config_schema('add', 's4') == 'OK'
    assert rediscli.config_schema('add', 's5') == 'OK'

    schema = rediscli.config_schema('get', 's3')
    assert schema
    print('schema s3:')
    print(schema)

    show =  rediscli.config_schema('show')
    assert show
    print('show schemas:')
    print(show)

    test_config_column()
    assert rediscli.config_schema('addfilter', 's5', 'c1', 'advancedflag') == 'OK'
    assert rediscli.config_schema('delfilter', 's5', 'advancedflag') == 'OK'
    assert rediscli.config_column('del', 's5', 'c1') == 'OK'
    assert rediscli.config_schema('del', 's5') == 'OK'


def test_config_column():
    assert rediscli.config_column('add', 's3', 'c1', 1)
    assert rediscli.config_column('add', 's3', 'c2', 2)
    assert rediscli.config_column('add', 's4', 'c1', 1)
    assert rediscli.config_column('add', 's4', 'c2', 2)
    assert rediscli.config_column('add', 's5', 'c1', 2)

#schema -> column -> vadd -> vmerge,vcount,vcard,vrange ->vrem,vremrange
test_config_schema()
test_vadd()
test_vmerge()
test_vcount()
test_vcard()
test_vrange()
test_vrem()
test_vremrange()