__author__ = 'sunlei'

import redis

rediscli = redis.Redis(host='10.77.109.117', port=6379, db=0)

"""
same for s3

redis 127.0.0.1:6379> config schema add s4
OK
redis 127.0.0.1:6379> config column add s4 c1 1
OK
redis 127.0.0.1:6379> config column add s4 c2 2

"""
def test_vadd():
    rediscli.vadd('123.s3', 1406870503888888, 1, 9)
    rediscli.vadd('123.s3', 1406870503888889, 1, 10)
    rediscli.vadd('123.s3', 1406870503888890, 1, 11)

    rediscli.vadd('123.s4', 1406870503888888, 1, 19)
    rediscli.vadd('123.s4', 1406870503888889, 1, 10)
    rediscli.vadd('123.s4', 1406870503888891, 1, 11)
    rediscli.vadd('123.s4', 1406870503888892, 1, 12)

def test_vcard():
    card1 =  rediscli.vcard('123.s3')
    card2 =  rediscli.vcard('123.s4')
    print(card1, card2)
    #return card1, card2

def test_vrem():
    card = rediscli.vcard('123.s4')
    print(card)
    rediscli.vrem('123.s4', 1406870503888888)
    card = rediscli.vcard('123.s4')
    print(card)

def test_vremrange():
    print(rediscli.vcard('123.s3'))
    rediscli.vremrange('123.s3', 1406870503888888, 1406870503888890)
    print(rediscli.vcard('123.s3'))


def test_vrange():
    rangeresult = rediscli.vrange('123.s3', '123.s3', 0, 1406870503888888, 1406870503888890)
    for item in rangeresult:
        print(item)

def test_vcount():
    print(rediscli.vcount('123.s3', 1406870503888888, 1406870503888890))
    print(rediscli.vcount('123.s3', 1406870503888888, 1406870503888889))

    print(rediscli.vcount('123.s4', 1406870503888888, 1406870503888892))
    print(rediscli.vcount('123.s4', 1406870503888892, 1406870503888889))

def test_vmerge():
    mergeresult = rediscli.vmerge('123.s3', '123.s4', 0, 100, 0, 1406870503888892)
    for item in mergeresult:
        print(item)
    print('\n')
    mergeresult = rediscli.vmerge('123.s3', '123.s4', 0, 100, 1406870503888890, 1406870503888892)
    for item in mergeresult:
        print(item)


#test_vadd()
test_vmerge()
#test_vcount()
#test_vcard()
#test_vrem()
#test_vrange()
#test_vremrange()