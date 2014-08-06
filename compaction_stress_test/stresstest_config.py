__author__ = 'sunlei 2014.08.04'
__doc__ = 'Global config of stress test vector'


class config():
    #key range:(start, offset)
    keyrange = (1, 2000000)

    #id_meta range:(start, offset)
    id_metarange = (1406870503888888, 2000000)

    #schema #
    schemarange = 2

    #column #
    columnrange = 4


    #total read thread
    nreadThread = 5
    #total write thread
    nwriteThread = 2

    #hosts and ports
    host1 = '10.77.109.117'
    port1 = 6379
    host2 = '10.77.109.117'
    port2 = 6380

    #log will record every command executed by redis
    #True if need log
    enable_log = False

    #sleep before reading, make sure there's initial data for read
    #time unit is second
    sleep_before_reading = 5

    #log file name
    log_file = 'stress_test_commands.log'
















