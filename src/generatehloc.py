#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module provides processing to generate HLOC(High, Low, Open, Close).
"""

import os
import argparse
import pandas as pd
from datetime import datetime as dt
import datetime
import logger


class GenerateHLOC(object):
    """
    This class generate HLOC.
    """

    def __init__(self, root_logger, input_dir, time_axis):
        # type: (logger, str, str) -> None
        """
        Class initialization.
        """

        self.logger = root_logger
        self.input_dir = input_dir
        # 作りたい足を指定
        self.time_axis = time_axis
        # 時間軸, 1分足, 1時間足, 日足
        self.time_list = ['one_minute', 'one_hour', 'one_day']

    def run(self):
        # type: (None) -> None
        """
        This pipeline read btc data frame and generate hloc.
        :return:
        """

        # 引数の時間軸チェック
        if not self.time_axis in self.time_list:
            self.logger.logger.error('Please specify time axis as one_minute, one_hour or one_day.')
            exit(1)

        # 入力データのディレクトリチェック
        if not os.path.exists(self.input_dir):
            self.logger.logger.error('Does not exist input directory: {}'.format(self.input_dir))
            exit(1)

        self.logger.logger.info('START generate hloc')
        self.logger.logger.info('time axis: {}'.format(self.time_axis))
        self.logger.logger.info('input directory: {}'.format(self.input_dir))

        # 指定されたディレクトリからファイルを取得
        file_list = os.listdir(self.input_dir)

        for file_name in file_list:
            # ディレクトリに存在するファイルを一つずつ読み込む
            df_btc = self.load_btc_data(file_name)

            # 時間を取り出しISOから日本時間に直しリストへ格納
            date_list = []
            exec_date = df_btc['exec_date']
            date_list = self.format_date(exec_date, date_list)

            # 引数で指定された時間軸でデータを揃える
            if self.time_axis in 'one_minute':
                # 1分足
                time_axis_date_list = self.get_one_minute_datetime(date_list)
            elif self.time_axis in 'one_hour':
                # 1時間足
                time_axis_date_list = self.get_one_hour_datetime(date_list)
            elif self.time_axis in 'one_day':
                # 日足
                time_axis_date_list = self.get_one_day_datetime(date_list)
            else:
                self.logger.logger.error('Unexpected time axis is specified : {}'.format(self.time_axis))
                exit(1)

            # 元のdatetimeを削除し時間軸を変換したデータを格納
            adjust_df_btc = df_btc.drop('exec_date', axis=1)
            adjust_df_btc['datetime'] = time_axis_date_list

            # hlocと出来高をを取得
            df_hloc = self.generate_hloc(adjust_df_btc)

            # 保存
            self.save_hloc_data(df_hloc)

    def get_one_minute_datetime(self, date_list):
        # type: (list) -> list
        """
        Convert seconds to 0
        :param date_list: date time list
        :return: list after conversion
        """
        self.logger.logger.info('Convert to 1 minute data')
        return [x.replace(second=0) for x in date_list]

    def get_one_hour_datetime(self, date_list):
        # type: (list) -> list
        """
        Convert seconds and minute to 0
        :param date_list: date time list
        :return: list after conversion
        """
        self.logger.logger.info('Convert to 1 hour data')
        temp_list = [x.replace(second=0) for x in date_list]
        return [x.replace(minute=0) for x in temp_list]

    def get_one_day_datetime(self, date_list):
        # type: (list) -> list
        """
        Convert seconds, minute and houe to 0
        :param date_list: date time list
        :return: list after conversion
        """
        self.logger.logger.info('Convert to 1 day data')
        temp_list = [x.replace(second=0) for x in date_list]
        temp_list = [x.replace(minute=0) for x in temp_list]
        return [x.replace(hour=0) for x in temp_list]

    def generate_hloc(self, adjust_df_btc):
        """
        Group data frames with datetime.
        Then, high, low, open, close, volume are acquired and stored.
        reference
        http://www.madopro.net/entry/bitcoin_chart
        :param adjust_df_btc: Data frame after time axis adjustment
        :return: Data frame storing hloc and volume every time
        """
        self.logger.logger.info('generate hloc')

        # ベースとなるデータフレーム作成と安値取得
        summary = adjust_df_btc[['datetime', 'price']].groupby(['datetime']).min().rename(columns={'price': 'min'})

        # 高値を取得しマージ
        summary = summary.merge(
            adjust_df_btc[['datetime', 'price']].groupby(['datetime']).max().rename(columns={'price': 'max'}),
            left_index=True, right_index=True)

        # 始値を取得しマージ
        summary = summary.merge(
            adjust_df_btc[['datetime', 'price']].groupby(['datetime']).last().rename(columns={'price': 'first'}),
            left_index=True, right_index=True)

        # 終値を取得しマージ
        summary = summary.merge(
            adjust_df_btc[['datetime', 'price']].groupby(['datetime']).first().rename(columns={'price': 'last'}),
            left_index=True, right_index=True)

        # 出来高を取得しマージ
        summary = summary.merge(
            adjust_df_btc[['datetime', 'size']].groupby(['datetime']).sum(),
            left_index=True, right_index=True)

        return summary

    def save_hloc_data(self, df_hloc):
        # type: (df) -> None
        """
        save HLOC data
        :param df_hloc: data frame(contain size)
        :return:
        """
        # TODO move util
        # ファイル名作成
        str_first_date = str(df_hloc.index[0]).replace(' ', '-')
        str_last_date = str(df_hloc.index[-1]).replace(' ', '-')
        file_name = './data/hloc/hloc_{}_{}_{}.csv'.format(self.time_axis, str_first_date, str_last_date)

        # 保存
        df_hloc.to_csv(file_name)
        self.logger.logger.info(' save on {}'.format(file_name))

    def load_btc_data(self, file_name):
        """
        Load btc data.
        :return: btc data frame
        """
        # TODO move util
        input_path = os.path.join(self.input_dir, file_name)
        df_btc = pd.read_csv(input_path)
        self.logger.logger.info('Load btc file: {}'.format(input_path))
        return df_btc

    @staticmethod
    def format_date(df_date, date_list):
        """
        Format string date as datetime.
        :param df_date: btc execute date data frame
        :param date_list: empty list
        :return: datetime formatted string date
        """
        # TODO move util
        for str_date in df_date:
            tmp_date = str_date.replace('T', ' ')
            tmp_date = tmp_date.split('.')
            tmp_date = tmp_date[0]
            date = dt.strptime(tmp_date, '%Y-%m-%d %H:%M:%S')
            # Adjust ISO format to Japan time
            date = date + datetime.timedelta(hours=9)
            date_list.append(date)
        return date_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', help='directory name',
                        action='store',
                        required=True)
    parser.add_argument('-t', '--time', help='time axis',
                        action='store',
                        required=True)

    args = parser.parse_args()
    logger = logger.Logger()

    generate_hloc = GenerateHLOC(logger, args.dir, args.time)
    generate_hloc.run()
