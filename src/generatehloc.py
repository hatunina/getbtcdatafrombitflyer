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
        # 時間軸, 1分足, 5分足, 1時間足, 日足
        self.time_list = ['one_minute', '5_minute', 'one_hour', 'one_day']

        self.columns =['datetime','min', 'max', 'first', 'last', 'size']
        self.file_lines = 500000

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

        summary_hloc = pd.DataFrame(columns=self.columns)
        summary_hloc = summary_hloc.set_index('datetime')

        # 指定されたディレクトリからファイルを取得
        file_list = os.listdir(self.input_dir)

        for file_name in file_list:
            # ディレクトリに存在するファイルを一つずつ読み込む
            df_btc = self.load_btc_data(file_name)
            
            # 5分足, TODO ちゃんと書く
            if self.time_axis in '5_minute':
                df_btc['exec_date'] = df_btc['exec_date'].map(self.format_date2)
                tmp_df = df_btc[['exec_date', 'price']]
                datetime_index = pd.DatetimeIndex(tmp_df['exec_date'])                                                                                                                                                                        
                tmp_df.index = datetime_index
                tmp_df = tmp_df.drop('exec_date', axis=1)
                
                # mean は特に意味はない(直でindexwp取得するとFutureWarningが出るため)
                hloc_index = tmp_df.resample('5T').mean().index
                max = tmp_df.resample('5T').max()
                min = tmp_df.resample('5T').min()
                first = tmp_df.resample('5T').first()
                last =  tmp_df.resample('5T').last()
                
                df_size = df_btc[['exec_date', 'size']]
                df_size.index = datetime_index
                df_size = df_size.drop('exec_date', axis=1)
                size = df_size.resample('5T').sum()
                
                df_hloc = pd.DataFrame(index=hloc_index)
                df_hloc = df_hloc.join(max)
                df_hloc = df_hloc.rename(columns={'price': 'max'})
                df_hloc = df_hloc.join(min)
                df_hloc = df_hloc.rename(columns={'price': 'min'})
                df_hloc = df_hloc.join(first)
                df_hloc = df_hloc.rename(columns={'price': 'first'})
                df_hloc = df_hloc.join(last)
                df_hloc = df_hloc.rename(columns={'price': 'last'})
                df_hloc = df_hloc.join(size)
                df_hloc['datetime'] = df_hloc.index
              
                summary_hloc = self.summarize_hloc(summary_hloc, df_hloc)

                continue

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

            # hlocデータをまとめる
            summary_hloc = self.summarize_hloc(summary_hloc, df_hloc)

        # まとめたデータをfile_lines(デフォルトは500,000)で分ける
        summary_hloc_list = self.separate_summary(summary_hloc)

        # 保存
        for separate_summary in summary_hloc_list:
            separate_summary = separate_summary.sort_index()
            self.save_hloc_data(separate_summary)


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
        df_hloc = adjust_df_btc[['datetime', 'price']].groupby(['datetime']).min().rename(columns={'price': 'min'})

        # 高値を取得しマージ
        df_hloc = df_hloc.merge(
            adjust_df_btc[['datetime', 'price']].groupby(['datetime']).max().rename(columns={'price': 'max'}),
            left_index=True, right_index=True)

        # 始値を取得しマージ
        df_hloc = df_hloc.merge(
            adjust_df_btc[['datetime', 'price']].groupby(['datetime']).last().rename(columns={'price': 'first'}),
            left_index=True, right_index=True)

        # 終値を取得しマージ
        df_hloc = df_hloc.merge(
            adjust_df_btc[['datetime', 'price']].groupby(['datetime']).first().rename(columns={'price': 'last'}),
            left_index=True, right_index=True)

        # 出来高を取得しマージ
        df_hloc = df_hloc.merge(
            adjust_df_btc[['datetime', 'size']].groupby(['datetime']).sum(),
            left_index=True, right_index=True)

        return df_hloc

    def summarize_hloc(self, summary_hloc, df_hloc):
        self.logger.logger.info('summarize_hloc')
        summary_hloc = pd.concat([summary_hloc, df_hloc])
        self.logger.logger.info('summary lines: {}'.format(len(summary_hloc)))
        return summary_hloc

    def separate_summary(self, summary_hloc):
        self.logger.logger.info('separate_summary')
        summary_hloc_list = []

        for file_num in range(int(len(summary_hloc)/self.file_lines)+1):
            # スライスでfile_linesごとに分ける
            tmp_summary = summary_hloc[self.file_lines*file_num:self.file_lines*(file_num+1)]
            summary_hloc_list.append(tmp_summary)
            self.logger.logger.info('separated: {}'.format(file_num))

        return summary_hloc_list

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
        file_name = './hloc/hloc_{}_{}_{}.csv'.format(self.time_axis, str_first_date, str_last_date)

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
        
    @staticmethod
    def format_date2(date):
        date = date.replace('T', ' ')
        date = date.split('.')
        date = date[0]
        date = dt.strptime(date, '%Y-%m-%d %H:%M:%S')
        date = date + datetime.timedelta(hours=9)
        return date


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', help='directory name',
                        action='store',
                        required=True)
    parser.add_argument('-t', '--time', help='time axis',
                        action='store',
                        required=True)
                        
    assert os.path.exists('./hloc'), 'Please make directry: hloc directory'
    
    args = parser.parse_args()
    logger = logger.Logger()

    generate_hloc = GenerateHLOC(logger, args.dir, args.time)
    generate_hloc.run()
