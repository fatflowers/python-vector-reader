__author__ = 'sunlei 2014.08.08'
__doc__ = 'Process of test_stress_vector.py dismissed after working for 6 hours' \
          'this file is to start a new test_stress_vector process when it is DEAD' \
          '' \
          'This program will check test_stress_vector every 10 second'

import time
import os
import logging
from stresstest_config import config


def run():
    pwd = os.popen('pwd').read().strip() + '/'

    logging.basicConfig(filename=config.helper_log_file,
                        format='%(asctime)s - %(threadName)s- %(funcName)s: %(message)s')
    helper_logger = logging.getLogger('helper_log')
    helper_logger.setLevel(logging.INFO)
    try:
        os.system('python ' + pwd + 'test_stress_vector.py &')
        helper_logger.info('test_stress_vector started')
    except Exception as err:
        a = 1
        print(err)
    while True:
        #check if the stress test is working,
        #run_count will be '0' if it's not
        run_count = os.popen(
            'ps -fe | grep "python" | grep "test_stress_vector" | grep -v "grep" | wc -l').read().strip()
        if run_count == '0':
            helper_logger.info('start a new test_stress_vector process after it is DEAD')
            os.system('python ' + pwd + 'test_stress_vector.py getup &')

        time.sleep(10)


run()


