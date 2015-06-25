#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@program:    NLS Module test case
@author:     Ahmed Hefnawi (@draco003)
@date:       June, 2015
@client:     Klaus
"""

from NLS import NLS
from datetime import datetime
from pytz import timezone

if __name__ == '__main__':
    time_last_started = datetime.now(timezone('US/Eastern'))

    # create an NLS instance
    nasdaq = NLS("AAPL")
    # truncates all data / clears `aapl` MySQL table
    items = nasdaq.init_scrape()
    print 'Items:'
    print items

    if items:
        log_type = items[0]
        status = items[1]
        records_created = items[2]
        error_message = items[3]

        time_last_finished = datetime.now(timezone('US/Eastern'))
        # timedelta
        time_execution = time_last_finished - time_last_started
        time_last_started = time_last_started.strftime('%Y-%m-%d %H:%M:%S')
        time_last_finished = time_last_finished.strftime('%Y-%m-%d %H:%M:%S')

        print 'Database Check:'
        print nasdaq.db_check()

        print 'Save Aggregate Data...'
        nasdaq.do_aggregate()

        # log to MySQL aapl_log
        print nasdaq.db_logger(tuple([log_type, str(time_last_started), str(time_last_finished),
                               str(time_execution), records_created, status, error_message]))
    else:
        print 'Market Offline!'
