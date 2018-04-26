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

    def __init__(self, start_date, finish_date, root_logger, before_id=0, count=500, file_lines=500000):
        self.logger = root_logger
        self.arg_before_id = before_id
        self.count = count
        self.file_lines = file_lines
        self.domain_url = 'https://api.bitflyer.jp'
        self.execution_history_url = '/v1/getexecutions'
        self.execution_history_params = {'count': self.count, 
                                            'before': self.arg_before_id, 
                                            'product_code': 'FX_BTC_JPY'}
                                            
        self.health_check_params = {'product_code': 'FX_BTC_JPY'}

        self.keys = ['id',
                     'side',
                     'price',
                     'size',
                     'exec_date',
                     'buy_child_order_acceptance_id',
                     'sell_child_order_acceptance_id']

        self.arg_start_date = start_date
        self.arg_finish_date = finish_date
        self.first_date = dt.strptime('2015-06-24 05:58:00', '%Y-%m-%d %H:%M:%S')
        self.change_num_base = 500
        self.target_start_id = None
        self.is_searching_before_id = True

    def run(self):
        # type: () -> None
        """
        指定されたデータ取得開始日と取得終了日のbefore_idを検索し、そのbefore_idでAPIを叩く
        :return:
        """

        # データ取得開始日が最初の取引より前の場合はエラー
        if self.is_arg_date_too_past():
            self.logger.logger.error('A date in the past is specified from the first deal. '
                  'Please specify a date after the date below.')
            self.logger.logger.error('The first deal date: {}'.format(self.first_date))
            exit(1)

        self.logger.logger.info('start date: {}'.format(self.arg_start_date))
        self.logger.logger.info('finish date: {}'.format(self.arg_finish_date))
        self.logger.logger.info('Look for the before id of the start date')

        # 検索開始のbefore_idを初期化
        search_before_id = 0

        # データ取得開始日のbefore_idを検索
        while self.is_searching_before_id:
            time.sleep(0.2)
            try:
                # before_idを検索し更新する
                search_before_id = self.search_before_id_pipeline(self.arg_start_date, search_before_id)
                # データ取得件数をリセット
                self.execution_history_params['count'] = self.count
            except Exception as e:
                # before_idの前に取得件数分のデータがない場合はエラー？サーバが忙しい？
                # または、APIを叩いた際にデータが取得できずpandasに変換してしまった場合のキャッチ
                # スリープしcountを調整して再度ループ
                self.logger.logger.error(e)
                # サーバのステータスチェック
                status = requests.get('https://api.bitflyer.jp/v1/gethealth', params=self.health_check_params)
                self.logger.logger('server status: {}'.format(status.text))
                time.sleep(10)
                random_rate = random.random()
                self.execution_history_params['count'] = 1 + int(self.count*random_rate)
                self.logger.logger.error('next use count: {}'.format(self.execution_history_params['count']))

        # 発見した取得開始日を保持. このidまで遡ってデータを取得する
        self.target_start_id = search_before_id
        self.logger.logger.info('The id of the date to be searched was found: {}'.format(self.target_start_id))

        # データ取得終了日の初期化
        search_finish_id = 0

        # 引数で指定されていればfinish_dateのidを探す
        if self.arg_finish_date:
            self.logger.logger.info('Look for the before id of the finish date')
            # フラグを初期化
            self.is_searching_before_id = True

            while self.is_searching_before_id:
                time.sleep(0.2)
                try:
                    search_finish_id = self.search_before_id_pipeline(self.arg_finish_date, search_finish_id)
                    self.execution_history_params['count'] = self.count
                except Exception as e:
                    self.logger.logger.error(e)
                    # サーバのステータスチェック
                    status = requests.get('https://api.bitflyer.jp/v1/gethealth', params=self.health_check_params)
                    self.logger.logger('server status: {}'.format(status.text))
                    time.sleep(10)
                    random_rate = random.random()
                    self.execution_history_params['count'] = 1 + int(self.count * random_rate)
                    self.logger.logger.error('next use count: {}'.format(self.execution_history_params['count']))
            self.logger.logger.info('The id of the finish date to be searched was found: {}'.format(search_finish_id))

        # finish_dateが指定されていれば、上の処理で探したid. 指定されていなければ0
        self.execution_history_params['before'] = search_finish_id
        self.execution_history_params['count'] = self.count

        # 見つかったbefore_id(データ取得日)までデータを取得する.
        # データ取得終了日が指定されている場合は上記の処理で見つかったbefore_id(データ取得終了日)から取得を開始する
        while True:
            # データ格納用データフレームの初期化
            df = pd.DataFrame(columns=self.keys)
            result_df = pd.DataFrame(columns=self.keys)
            # プログレスバーの初期化
            p = ProgressBar(len(result_df), self.file_lines)
            while True:
                # データ取得, データフレームへ変換
                try:
                    time.sleep(0.2)
                    response = self.execute_api_request()
                    tmp_df = pd.read_json(response.text)
                except Exception as e:
                    self.logger.logger.error(' An error occurred in api request: {}'.format(response))
                    self.logger.logger.error(e)
                    # サーバのステータスチェック
                    status = requests.get('https://api.bitflyer.jp/v1/gethealth', params=self.health_check_params)
                    self.logger.logger.error('server status: {}'.format(status.text))
                    time.sleep(10)
                    random_rate = random.random()
                    self.execution_history_params['count'] = 1 + int(self.count * random_rate)
                    self.logger.logger.error('next use count: {}'.format(self.execution_history_params['count']))
                    continue

                # 次のループの設定
                next_before_id = tmp_df['id'].iloc[-1]
                self.execution_history_params['before'] = next_before_id
                self.execution_history_params['count'] = self.count

                # 取得したデータを足し合わせる
                df = pd.concat([df, tmp_df])

                # 発見したbefore_id(データ取得開始日)を通り過ぎていないかチッェク
                # 通り過ぎていたら、そこまでをresult_dfへ格納
                result_df = df[df['id'] >= self.target_start_id]

                # データ数がfile_linesを上回っていないかチェック
                # また、発見したbefore_id(データ取得開始日)を通り過ぎていないかチッェクし通り過ぎていたら、ループを抜ける
                if len(result_df) >= self.file_lines or df.shape[0] > result_df.shape[0]:
                    p.update(self.file_lines)
                    break
                else:
                    p.update(len(result_df))

            # 一つのファイルを作ったら保存
            self.save_result_data(result_df)

            # 発見したbefore_id(データ取得開始日)を過ぎていたらループ自体を終了
            if df.shape[0] > result_df.shape[0]:
                break

        self.logger.logger.info('FINISH getbtc')

    def search_before_id_pipeline(self, base_date, search_before_id):
        # type: (dt, int) -> int
        """
        before_idを検索するパイプライン. APIを叩き得られたデータの日付と指定された日付の差を元にbefore_idを更新する
        :param base_date: 検索する基準となる日付. 開始日 or 終了日
        :param search_before_id: 検索する基準となるbefore_id
        :return: 見つかったbefore_id
        """
        # before_idをセット
        self.execution_history_params['before'] = search_before_id

        try:
            # APIを叩く
            search_response = self.execute_api_request()
            # データをpandasに変換
            search_btc_df = pd.read_json(search_response.text)
        except Exception as e:
            # 500エラーが出た際のハンドリング（pandas変換時にエラー）. 上に投げる
            self.logger.logger.error(' An error occurred in api request: {}'.format(search_response))
            self.logger.logger.error(e)

        # ISOを日本時間に変換
        search_date = self.format_date(search_btc_df['exec_date'].iloc[0])
        self.logger.logger.info('searching for date: {}'.format(search_date))

        # 基準日と今回得られた日付の差を元に次に検索するbefore_idを決定する
        search_before_id = self.search_before_id(base_date, search_date, search_btc_df)

        return search_before_id

    def search_before_id(self, base_date, search_date, search_btc_df):
        # type: (dt, dt, df) -> int
        """
        基準日と渡された日付の大小を調べ、before_idを増減させて返す
        :param base_date: 検索する基準となる日付. 開始日 or 終了日
        :param search_date: 現在のbefore_idで得られた日付
        :param search_btc_df: 現在のbefore_idで得られたデータフレーム
        :return: 増減させたbefore_id
        """
        # 基準日が未来にある場合(遡りすぎた)
        if search_date < base_date:
            # 日付時刻の差を求める
            diff_second = (base_date - search_date).total_seconds()
            # diff_secondを使ってbefore_idの増減数を決定する
            change_id_num = self.get_change_id_num(diff_second)
            # 次のbefore_idを決定する
            search_before_id = int(search_btc_df['id'].iloc[0]) + change_id_num

        # 基準日が過去にある場合
        elif search_date > base_date:
            # 日付時刻の差を求める
            diff_second = (search_date - base_date).total_seconds()
            # diff_secondを使ってbefore_idの増減数を決定する
            change_id_num = self.get_change_id_num(diff_second)
            # 次のbefore_idを決定する
            search_before_id = int(search_btc_df['id'].iloc[-1]) - change_id_num

        # 基準日まで遡った場合
        else:
            # 日本時刻に直し~時~分の形式に直す
            iso_arg_date = base_date - datetime.timedelta(hours=9)
            iso_arg_date = str(iso_arg_date.hour) + ':' + str(iso_arg_date.minute)

            # 基準日（時刻）のidを調べる
            for i, date in enumerate(search_btc_df['exec_date']):
                if iso_arg_date in date:
                    tmp_series = search_btc_df.iloc[i]
                    # idを取り出す
                    search_before_id = tmp_series['id']

            # 検索終了フラグへ書き換え
            self.is_searching_before_id = False

        return search_before_id

    def get_change_id_num(self, diff_second):
        # type: (float) -> int
        """
        日付時刻の差を元にbefore_idの増減させる数字を決める
        :param diff_second: 日付時刻の差
        :return: before_idの増減数
        """
        # 各時間軸に直す
        diff_minutes = diff_second/60
        diff_hour = diff_minutes/60
        diff_date = diff_hour/24
        diff_week = diff_date/7
        diff_month = diff_week/4
        diff_year = diff_month/12

        # 各時間軸で増減数を決定する. 過去になればなるほど取引量が少なくなるので、
        # 数年前、数ヶ月前だからと言って大きく増減させるとうまくいかない
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
        # type: () -> str
        """
        bitflyerAPIを叩く
        :return: btcデータ
        """
        request_url = self.domain_url + self.execution_history_url
        return requests.get(request_url, params=self.execution_history_params)

    def is_arg_date_too_past(self):
        # type: () -> bool
        """
        データ取得開始日のチェック
        :return: bool
        """
        return self.first_date > self.arg_start_date

    def save_result_data(self, result_df):
        # type: (df) -> None
        """
        btcデータを保存する
        :param result_df: データを格納したデータフレーム
        :return:
        """
        # 日付フォーマット
        first_date = self.format_date(result_df['exec_date'].iloc[0])
        last_date = self.format_date(result_df['exec_date'].iloc[-1])

        # ファイル名作成
        str_first_date = str(first_date).replace(' ', '-').replace(':00', '')
        str_last_date = str(last_date).replace(' ', '-').replace(':00', '')
        file_name = './data/btc_{}_{}.csv'.format(str_first_date, str_last_date)

        # 保存
        result_df.to_csv(file_name, index=False)
        self.logger.logger.info(' save on {}'.format(file_name))

    @staticmethod
    def format_date(date_line):
        # type: (str) -> dt
        """
        ISOフォーマットを日本時間へ変換
        :param date_line: ISO形式の日付
        :return: 日本時間へ変換した日付
        """
        tmp_date = date_line.replace('T', ' ')
        tmp_date = tmp_date.split('.')[0]
        date = dt.strptime(tmp_date, '%Y-%m-%d %H:%M:%S')
        date = date.replace(second=0)
        date = date + datetime.timedelta(hours=9)
        return date


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_date', help='Date of data you want. '
                                             'Please specify it in the following format. '
                                             'ex. 2018-04-07-22:06:00',
                        action='store',
                        required=True)
    parser.add_argument('-f', '--finish_date', help='It is the finish date. ',
                        action='store',
                        required=False)

    args = parser.parse_args()
    
    # ディレクトリチェック
    assert os.path.exists('./data'), print('Please make directry: data directory')
    assert os.path.exists('./log'), print('Please make directry: log dirctory')
    
    logger = logger.Logger()
    logger.logger.info('START getbtc')
    
    try:
        arg_start_date = dt.strptime(args.start_date, '%Y-%m-%d-%H:%M:%S')
        arg_start_date = arg_start_date.replace(second=0)
        if args.finish_date:
            arg_finish_date = dt.strptime(args.finish_date, '%Y-%m-%d-%H:%M:%S')
            arg_finish_date = arg_finish_date.replace(second=0)
        else:
            arg_finish_date = None
    except:
        # 引数の日付のフォーマットチェック
        logger.logger.error('The format of the date is incorrect. Please specify it in the following format.')
        logger.logger.error('ex. 2018-04-07-22:06:00')
        exit(1)

    # 引数の日付チェック（start_dateの方が「最近」だとエラー）
    if args.finish_date:
        if arg_start_date > arg_finish_date:
            logger.logger.error('Please specify the date after the start date for the finish date.')
            exit(1)

        # finish_dateが現在の時刻より未来だとエラー
        if arg_finish_date > dt.now():
            logger.logger.error('Future date can not be specified.')
            exit(1)

    get_btc = GetBtcDataFromBitflyer(arg_start_date, arg_finish_date, logger)
    get_btc.run()
