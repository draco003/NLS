#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@program:    Nasdaq Last Sale (NLS) scraper
@author:     Ahmed Hefnawi (@draco003)
@date:       June, 2015
@client:     Klaus
"""

import sys
import traceback
import re
import requests
from datetime import datetime
from pytz import timezone
from lxml import html
import MySQLdb as mdb


class NLS(object):
    """Nasdaq Last Sale Class"""

    def __init__(self, stock_name):
        """Class Initialization"""

        self.stock_name = str(stock_name).lower()
        # requests module session
        self.session = requests.Session()

        # MySQL data list
        self.data = list()

        # MySQL table names
        self.main_table = 'aapl'
        self.log_table = 'aapl_log'
        self.progress_table = 'aapl_progress'
        self.aggregate_table = 'aapl_second'

        # non decimal regex
        self.non_decimal = re.compile(r'[^\d.]+')

        self.error = ""
        self.status = ""
        self.market_time = 0
        self.sales_time = 1
        self.pages = 0

        self.host = "www.nasdaq.com"
        self.base_url = "".join(["http://", self.host, "/symbol"])

        # MySQL connection
        self.mysql_dbhost = "xx"
        self.mysql_dbname = "xx"
        self.mysql_dbuser = "xx"
        self.mysql_dbpass = "xx"

        self.__mysql_con = mdb.connect(self.mysql_dbhost, self.mysql_dbuser,
                                       self.mysql_dbpass, self.mysql_dbname)

    def __remove_non_ascii(self, text):
        return "".join(char for char in text if ord(char) < 128)

    def __get_page(self, url, ua=None, host=None, referer=None, proxies=None,
                   verbose=None, params=None, cookie=None,
                   cookies=None, timeout=150, json=None, gzip=None):

        """Download URL and return HTML/JSON.
           Otherwise return status code.
        """

        headers = {}
        if referer:
            headers["Referer"] = str(referer)
        if cookie:
            headers["Cookie"] = str(cookie)
        if ua:
            headers["User-Agent"] = str(ua)
        if host:
            headers["Host"] = str(host)
        if gzip:
            headers['Accept-Encoding'] = 'gzip'

        try:
            r = self.session.get(url,
                                 headers=headers,
                                 timeout=timeout,
                                 proxies=proxies,
                                 params=params,
                                 cookies=cookies,)

            # raise HTTPError on error codes
            r.raise_for_status()
            if r.status_code == requests.codes.ok:  # @UndefinedVariable
                if verbose:
                    print ' '.join(["Download OK:", r.url, "::", str(r.status_code)])
                # return r.status_code
                # return BeautifulSoup(r.text)
                # return removeNonAscii(r.text)
                if json:
                    return r.json()
                else:
                    return r.text

        except requests.exceptions.HTTPError:
            if verbose:
                print ' '.join(["HTTPError:", r.status_code])
            return r.status_code

        except requests.exceptions.ConnectionError:
            if verbose:
                print ' '.join(['ConnectionError:',
                                str(traceback.format_exception(*sys.exc_info()))])
            pass
        except Exception:
            if verbose:
                print ' '.join(['ErrorValue:',
                                str(traceback.format_exception(*sys.exc_info()))])
            pass
            return None

    def __update_vars(self):
        """Update Market Time AND Number of Pages"""
        try:
            # build URL for initial scraping
            url = ''.join([self.base_url, "/", self.stock_name, "/time-sales"])
            payload = {'time': self.sales_time}
            result_html = self.__get_page(url=url, ua="Mozilla/5.0",
                                          host=self.host, params=payload, verbose=True)
            r_tree = html.fromstring(result_html)
            # get last page number
            try:
                self.pages = int(r_tree.xpath('//a[@id="quotes_content_left_lb_LastPage"]/@href')[0].split("=")[-1])
            except:
                # single page
                self.pages = 0
            # market time = "Jun. 1, 2015 10:40 ET " parsing
            market_time = r_tree.xpath('//div[@id="qwidget_markettimedate"]/small/span[@id="qwidget_markettime"]/text()')[0].strip().replace("ET", "")
            market_time = self.__remove_non_ascii(market_time)
            # print market_time
            try:
                # includes hour, minute
                self.market_time = datetime.strptime(market_time.strip(), "%b. %d, %Y%H:%M")
            except:
                # no hour, minute
                self.market_time = datetime.strptime(market_time.strip(), "%b. %d, %Y")
            return True
        except:
            return None

    def __db_insert_data(self, truncate=None):
        try:
            # prepare aapl_progress statistics
            last_sale_time = self.sales_time
            last_pageno = self.pages
            last_time = datetime.now(timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
            records_partial = (len(self.data) % 50.0)
            # print (int(last_sale_time), int(last_pageno), int(records_partial), str(last_time))
            # insert records into MySQL DB
            # TODO: Extend MySQLdb class
            # http://mysql-python.sourceforge.net/MySQLdb.html#using-and-extending
            with self.__mysql_con:
                cur = self.__mysql_con.cursor()
                if truncate is True:
                    cur.execute('TRUNCATE TABLE `%s`' % self.main_table)

                query_text = """INSERT INTO `%s` (nls_time, nls_price, nls_volume) values (%%s, %%s, %%s)""" % self.main_table 
                cur.executemany(query_text, self.data)

                query_text = """INSERT INTO `%s` (sale_time, pageno, records_partial, last_time) values (%%s, %%s, %%s, %%s)""" % self.progress_table
                progress_data = tuple([int(last_sale_time), int(last_pageno), int(records_partial), str(last_time)]) 
                cur.execute(query_text, progress_data)

            # clear data list
            self.data = list()
            return True
        except:
            print str(traceback.format_exception(*sys.exc_info()))
            return None

    def __get_table(self, sales_time=1, page=1, partials=None):
        """Parse HTML Table into a Python list"""
        try:
            data = list()
            # build URL
            url = ''.join([self.base_url, "/", self.stock_name, "/time-sales"])
            payload = {'time': str(sales_time), 'pageno': str(page)}
            r_html = self.__get_page(url=url, ua="Mozilla/5.0", host=self.host, params=payload, verbose=True)
            r_tree = html.fromstring(r_html)
            # get table: #AfterHoursPagingContents_Table
            table = r_tree.xpath('//div[@class="genTable"]/table[@id="AfterHoursPagingContents_Table"]/tr')
            # check if market is online
            if not table:
                # No Data Available
                return None
            else:
                # get market data for all pages
                tds = table[0].xpath('//table[@id="AfterHoursPagingContents_Table"]/tr/td/text()')
                # group list into chunks of 3 elements
                n = 3
                group_tds = [tds[i:i + n] for i in range(0, len(tds), n)]
                if partials:
                    # return partial records only 
                    group_tds = group_tds[:int(partials)]

                for td in group_tds:
                    # #AfterHoursPagingContents_Table > tbody:nth-child(2) > tr:nth-child(1) > td:nth-child(1)
                    nls_time = "".join([str(int(self.market_time.year)), "-", str("%02d" % int(self.market_time.month)), "-", str("%02d" % int(self.market_time.day)), " ", str(td[0].strip())])
                    nls_price = td[1].strip().strip('$')
                    nls_volume = td[2].strip()
                    data.append(tuple([nls_time, float(self.non_decimal.sub('', nls_price)), int(nls_volume.replace(",", ""))]))
                    # print tuple([nls_time, float(self.non_decimal.sub('', nls_price)), int(nls_volume.replace(",", ""))])
                # return list of tuples
                return data
        except:
            return None

    def init_scrape(self):
        """Initial Scrape"""
        try:
            # records count
            data2 = 0
            # reset list, start with first page of time_sales=1 (9:30 - 9:59) EST
            self.data = list()
            self.sales_time = 1

            # update market time and number of pages
            self.__update_vars()

            if self.pages == 0:
                # market offline
                return None

            # Go Backwards!
            for page in xrange(self.pages, 0, -1):
                # start with first page of time_sales=1 (9:30 - 9:59) EST
                self.data.extend(self.__get_table(self.sales_time, page))

            # track records number before being cleared by __db_insert_data()
            data2 = data2 + len(self.data)
            # insert scraped data, truncate table as well
            self.__db_insert_data(truncate=True)

            self.error = 'N/A'
            self.status = 'success'
            return ['init', self.status, len(data2), self.error]

        except Exception:
            # empty list
            self.data = list()
            self.status = 'fail'
            self.error = str(traceback.format_exception(*sys.exc_info()))
            return None

    def update_scrape(self):
        """Update scraped data"""
        try:
            # records count
            data2 = 0
            # reset list
            self.data = list()
            # get latest records
            with self.__mysql_con:
                cur = self.__mysql_con.cursor(mdb.cursors.DictCursor)
                cur.execute("""SELECT * FROM `%s` ORDER BY id DESC LIMIT 1""" % self.progress_table)
                row = cur.fetchone()

                self.sales_time = row['sale_time']
                last_page = row['pageno']
                last_partials = row['records_partial']

            # update market time and number of pages
            self.__update_vars()
            print 'Last Page: ' + str(last_page)
            print 'Pages: ' + str(self.pages)

            # where to start
            start_page = self.pages - last_page
            if last_page == self.pages:
                # check total number of records
                # get new partials, 50 is not a partial but a full page
                new_partials = len(self.__get_table(self.sales_time, self.pages)) % 50
                new_records = (int(self.pages - 1) * 50) + int(new_partials)
                old_records = (int(last_page - 1) * 50) + int(last_partials)
                print 'Old records: %s, New records: %s' % (old_records, new_records)
                print 'Old partials: %s, New partials: %s' % (last_partials, new_partials)
                print 'Old pages: %s, New pages: %s' % (last_page, self.pages)
                if new_records > old_records:
                    print 'Get New Partials!'
                    # partial records exist
                    # scrape them from last_page
                    self.data.extend(self.__get_table(self.sales_time, last_page, last_partials))
                    data2 = data2 + len(self.data)
                    # insert scraped data
                    self.__db_insert_data()
                elif new_records == old_records:
                    print 'Jump to Next Sale!'
                    # jump to next sale time and check data
                    # no need to go deeper as this would be a cronjob
                    if self.sales_time < 13:
                        self.sales_time = self.sales_time + 1
                        # update market time and number of pages
                        self.__update_vars()
                        if self.pages == 0:
                            print 'Market Offline!'
                            # market offline, will be processed in the next run
                            pass
                        else:
                            print 'Scraping New Sale, going backwards!'
                            # Go Backwards!
                            for page in xrange(self.pages, 0, -1):
                                # start with first page of time_sales=1 (9:30 - 9:59) EST
                                self.data.extend(self.__get_table(self.sales_time, page))

                            data2 = data2 + len(self.data)
                            # insert scraped data
                            self.__db_insert_data()
                    else:
                        print 'Nothing New - End of Trading Day!'
                else:
                    print 'New records < Old records ?!'
                    pass
            else:
                print 'New Pages Available!'
                # new pages available, add them
                for page in xrange(start_page, 0, -1):
                    self.data.extend(self.__get_table(self.sales_time, page))

                data2 = data2 + len(self.data)
                # insert scraped data
                self.__db_insert_data()

            self.error = 'N/A'
            self.status = 'success'
            return ['update', self.status, data2, self.error]

        except Exception:
            # empty list?
            self.data = list()
            self.status = 'fail'
            self.error = str(traceback.format_exception(*sys.exc_info()))
            return None

    def do_aggregate(self):
        """Aggregate NLS Data into DB"""
        try:
            agg_data = list()
            with self.__mysql_con:
                # Dictionary Cursor
                cur = self.__mysql_con.cursor(mdb.cursors.DictCursor)
                # Handle aggregation in MySQL (Clean Method)
                cur.execute("""SELECT nls_time, nls_price, SUM(nls_volume) AS nls_agg FROM `%s` GROUP BY nls_time ORDER BY id DESC""" % self.main_table)
                rows = cur.fetchall()
                for row in rows:
                    nls_time = row['nls_time']
                    nls_price = row['nls_price']
                    nls_agg = row['nls_agg']
                    agg_data.append(tuple([nls_time, nls_price, nls_agg]))

                # insert aggregate data into `aapl_second` table
                cur.execute("""TRUNCATE TABLE `%s`""" % self.aggregate_table)
                query_text = """INSERT INTO `%s` (nls_time, nls_price, nls_volume) values (%%s, %%s, %%s)""" % self.aggregate_table
                cur.executemany(query_text, agg_data)
                return True

        except Exception:
            print str(traceback.format_exception(*sys.exc_info()))
            return None

    def db_check(self):
        """MySQL DB Check"""
        try:
            with self.__mysql_con:
                cur = self.__mysql_con.cursor()
                cur.execute("""SELECT * FROM `%s` ORDER BY id DESC LIMIT 1""" % self.progress_table)
                row = cur.fetchone()
                return row
        except Exception:
            return None

    def db_logger(self, data):
        """MySQL DB logger"""
        try:
            with self.__mysql_con:
                cur = self.__mysql_con.cursor()
                query_text = """INSERT INTO `%s`
                                (log_type,time_last_started,time_last_finished,time_execution,
                                records_created,status,error_message)

                                VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s)""" % self.log_table
                r = cur.execute(query_text, data)
                return r

        except Exception:
            return None

