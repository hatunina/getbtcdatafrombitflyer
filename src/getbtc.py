#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import argparse
import pandas as pd
from progressbar import ProgressBar
from datetime import datetime as dt
import datetime
import time
import random
import logger


class GetBtcDataFromBitflyer(object):

    def __init__(self, arg_date, logger, before_id=0, count=500, file_lines=500000):
        self.logger = logger
        self.arg_before_id = before_id
        self.count = count
        self.file_lines = file_lines
        self.domain_url = 'https://api.bitflyer.jp'
        self.execution_history_url = '/v1/getexecutions'
        self.execution_history_params = {'count': self.count, 'before': self.arg_before_id}

        self.keys = ['id',
                     'side',
                     'price',
                     'size',
                     'exec_date',
                     'buy_child_order_acceptance_id',
                     'sell_child_order_acceptance_id']

        self.arg_date = arg_date
        self.first_date = dt.strptime('2015-06-24 05:58:00', '%Y-%m-%d %H:%M:%S')
        self.change_num_base = 500
        self.target_date_id = None
        self.is_searching_before_id = True

    def run(self):
        if self.is_arg_date_too_past():
            self.logger.logger.error('A date in the past is specified from the first deal. '
                  'Please specify a date after the date below.')
            self.logger.logger.error('The first deal date: {}'.format(self.first_date))
            exit(1)

        search_before_id = 0

        self.logger.logger.info('START search for before id')
        self.logger.logger.info('arg date: {}'.format(self.arg_date))

        while self.is_searching_before_id:
            time.sleep(0.2)
            try:
                search_before_id = self.search_before_id_pipeline(search_before_id)
                self.execution_history_params['count'] = self.count
            except Exception as e:
                self.logger.logger.error(e)
                random_rate = random.random()
                self.execution_history_params['count'] = int(self.count*random_rate)
                self.logger.logger.error('next use count: {}'.format(self.execution_history_params['count']))

        self.target_date_id = search_before_id
        self.logger.logger.info('The id of the date to be searched was found: {}'.format(self.target_date_id))
        self.execution_history_params['before'] = 0
        self.execution_history_params['count'] = self.count

        while True:
            df = pd.DataFrame(columns=self.keys)
            result_df = pd.DataFrame(columns=self.keys)
            p = ProgressBar(len(result_df), self.file_lines)
            while True:
                try:
                    time.sleep(0.2)
                    response = self.execute_api_request()
                    tmp_df = pd.read_json(response.text)
                except Exception as e:
                    self.logger.logger.error(' An error occurred in api request: {}'.format(response))
                    self.logger.logger.error(e)
                    random_rate = random.random()
                    self.execution_history_params['count'] = int(self.count * random_rate)
                    self.logger.logger.error('next use count: {}'.format(self.execution_history_params['count']))
                    continue

                next_before_id = tmp_df['id'].iloc[-1]

                self.execution_history_params['before'] = next_before_id
                self.execution_history_params['count'] = self.count

                df = pd.concat([df, tmp_df])

                result_df = df[df['id'] >= self.target_date_id]

                if len(result_df) >= self.file_lines or df.shape[0] > result_df.shape[0]:
                    p.update(self.file_lines)
                    break
                else:
                    p.update(len(result_df))

                # if df.shape[0] > result_df.shape[0]:
                #     break

            self.save_result_data(result_df)

            if df.shape[0] > result_df.shape[0]:
                break

        self.logger.logger.info('FINISH getbtc')

    def search_before_id_pipeline(self, search_before_id):
        self.execution_history_params['before'] = search_before_id

        try:
            search_response = self.execute_api_request()
            search_btc_df = pd.read_json(search_response.text)
        except Exception as e:
            self.logger.logger.error(' An error occurred in api request: {}'.format(search_response))
            self.logger.logger.error(e)

        search_date = self.format_date(search_btc_df['exec_date'].iloc[0])
        self.logger.logger.info('searching for date: {}'.format(search_date))

        search_before_id = self.search_before_id(search_date, search_btc_df)

        return search_before_id

    def search_before_id(self, search_date, search_btc_df):
        if search_date < self.arg_date:
            diff_second = (self.arg_date - search_date).total_seconds()
            change_id_num = self.get_change_id_num(diff_second)
            search_before_id = int(search_btc_df['id'].iloc[0]) + change_id_num

        elif search_date > self.arg_date:
            diff_second = (search_date - self.arg_date).total_seconds()
            change_id_num = self.get_change_id_num(diff_second)
            search_before_id = int(search_btc_df['id'].iloc[-1]) - change_id_num

        else:
            iso_arg_date = self.arg_date - datetime.timedelta(hours=9)
            iso_arg_date = str(iso_arg_date.hour) + ':' + str(iso_arg_date.minute)
            for i, date in enumerate(search_btc_df['exec_date']):
                if iso_arg_date in date:
                    tmp_series = search_btc_df.iloc[i]
                    search_before_id = tmp_series['id']
            self.is_searching_before_id = False

        return search_before_id

    def get_change_id_num(self, diff_second):
        diff_minutes = diff_second/60
        diff_hour = diff_minutes/60
        diff_date = diff_hour/24
        diff_week = diff_date/7
        diff_month = diff_week/4
        diff_year = diff_month/12

        if diff_year >= 1:
            # 26,280,000
            # floatだとパラメータが認識されない
            change_id_num = int((self.change_num_base * 60 * 24 * 365) * 0.1)
        elif diff_month >= 1:
            # 4,464,000
            change_id_num = int((self.change_num_base * 60 * 24 * 31) * 0.1)
        elif diff_week >= 1:
            # 10080
            change_id_num = self.change_num_base * 60 * 24 * 7
        elif diff_date >= 1:
            change_id_num = self.change_num_base * 60 * 24
        elif diff_hour >= 1:
            change_id_num = self.change_num_base * 60
        elif diff_minutes >= 1:
            change_id_num = self.change_num_base
        elif diff_minutes == 0:
            change_id_num = 0

        return change_id_num

    def execute_api_request(self):
        request_url = self.domain_url + self.execution_history_url
        return requests.get(request_url, params=self.execution_history_params)

    def is_arg_date_too_past(self):
        return self.first_date > self.arg_date


    def format_date(self, date_line):
        tmp_date = date_line.replace('T', ' ')
        tmp_date = tmp_date.split('.')[0]
        date = dt.strptime(tmp_date, '%Y-%m-%d %H:%M:%S')
        date = date.replace(second=0)
        # Adjust ISO format to Japan time
        date = date + datetime.timedelta(hours=9)
        return date

    def save_result_data(self, result_df):
        first_date = self.format_date(result_df['exec_date'].iloc[0])
        last_date = self.format_date(result_df['exec_date'].iloc[-1])

        str_first_date = str(first_date).replace(' ', '-').replace(':00', '')
        str_last_date = str(last_date).replace(' ', '-').replace(':00', '')

        file_name = './data/btc_{}_{}.csv'.format(str_first_date, str_last_date)
        result_df.to_csv(file_name, index=False)
        self.logger.logger.info(' save on {}'.format(file_name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help='Date of data you want. '
                                             'Please specify it in the following format. '
                                             'ex. 2018-04-07-22:06:00',
                        action='store',
                        required=True)

    args = parser.parse_args()
    logger = logger.Logger()
    logger.logger.info('START getbtc')

    try:
        arg_date = dt.strptime(args.date, '%Y-%m-%d-%H:%M:%S')
        arg_date = arg_date.replace(second=0)
    except:
        logger.logger.error('The format of the date is incorrect. Please specify it in the following format.')
        logger.logger.error('ex. 2018-04-07-22:06:00')
        exit(1)

    get_btc = GetBtcDataFromBitflyer(arg_date, logger)
    get_btc.run()
