#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from progressbar import ProgressBar
from datetime import datetime as dt
import datetime


class PythonBitFlyerApp(object):

    def __init__(self, count=500, count_limit=20):
        # before id
        self.before_id = 0
        # bit data size
        self.count = count
        # loop size
        self.count_limit = count_limit
        # request url
        self.domain_url = 'https://api.bitflyer.jp'
        self.execution_history_url = '/v1/getexecutions'
        self.output_file_name = 'bit_data.csv'
        # column array
        self.keys = ['id',
                     'side',
                     'price',
                     'size',
                     'exec_date',
                     'buy_child_order_acceptance_id',
                     'sell_child_order_acceptance_id']

        self.first_time_flag = True

        self.execution_history_params = {'count': self.count,
                                    'before': self.before_id}

    def run(self):

        arg_date = dt.strptime('2018-04-05 23:32:56', '%Y-%m-%d %H:%M:%S')
        arg_date = arg_date.replace(second=0)

        while True:
            start_id = self.search_start_id(arg_date)
            self.execution_history_params['before'] = start_id


        # init ProgressBar
        p = ProgressBar(0, self.count_limit)

        # init DataFrame
        df = pd.DataFrame(columns=self.keys)

        for progress_num in range(self.count_limit):
            # request execution history
            response = self.execute_api_request(self.execution_history_url, self.execution_history_params)
            btc_list = response.json()

            last_id = btc_list[-1]['id']

            # update parameters with last id
            self.execution_history_params['before'] = last_id

            # show execution progress
            p.update(progress_num)

            tmp_df = pd.read_json(response.text)
            df = pd.concat([df, tmp_df])

        self.save_result_data(df)

    # execute api request
    def execute_api_request(self, url, params):
        request_url = self.domain_url + url
        return requests.get(request_url, params=params)

    def save_result_data(self, result_df):
        result_df.to_csv(self.output_file_name, index=False)
        print('save on {}'.format(self.output_file_name))

    def search_start_id(self, arg_date):
        # request execution history
        response = self.execute_api_request(self.execution_history_url, self.execution_history_params)

        search_btc_df = pd.read_json(response.text)


        str_date = search_btc_df['exec_date'].iloc[0]
        tmp_date = str_date.replace('T', ' ')
        tmp_date = tmp_date.split('.')[0]
        date = dt.strptime(tmp_date, '%Y-%m-%d %H:%M:%S')
        date = date.replace(second=0)

        # Adjust ISO format to Japan time
        date = date + datetime.timedelta(hours=9)

        if date < arg_date:
            # 引数のdateは今取得したbtcデータよりも未来にある
            # 初回は本当に未来のdateを指定されているのでエラー
            # ２回目以降は過去に戻りすぎなのでidを増やす
            if self.first_time_flag:
                return 'miss'
            else:
                if (arg_date - date).total_seconds() <= 300:
                    start_id = int(search_btc_df['id'][0]) + 500
                else:
                    start_id = int(search_btc_df['id'][499]) + 5000
                self.first_time_flag = False
                return start_id
        elif date > arg_date:
            # 引数のdateは今取得したbtcデータよりも過去にある
            if (date - arg_date).total_seconds() <= 300:
                start_id = int(search_btc_df['id'][499]) - 500
            else:
                start_id = int(search_btc_df['id'][0]) - 5000
            self.first_time_flag = False
            return start_id
        else:
            iso_arg_date = arg_date - datetime.timedelta(hours=9)
            iso_arg_date = str(iso_arg_date.hour) + ':' + str(iso_arg_date.minute)
            for i, date in enumerate(search_btc_df['exec_date']):
                if iso_arg_date in date:
                    tmp_series = search_btc_df.iloc[i]

            return tmp_series['id']


if __name__ == '__main__':
    python_bit_flyer_app = PythonBitFlyerApp()
    python_bit_flyer_app.run()
