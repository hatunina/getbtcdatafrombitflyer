#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module plots hloc data.
"""

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick2_ohlc, volume_overlay
import logger


class PlotChart(object):
    """
    This class plot the hloc btc data.
    """

    def __init__(self, root_logger, input_file):
        # type: (logger, str) -> None
        """
        Class initialization
        """

        self.logger = root_logger
        self.input_file = input_file

    def run(self):
        # type: () -> None
        """
        Read and plot the hloc file specified by the argument.
        :return:
        """

        # ファイル存在チェック
        if not os.path.exists(self.input_file):
            self.logger.logger.error('Does not exist input file: {}'.format(self.input_file))
            exit(1)

        # hlocデータをロード
        df_btc = self.load_btc_data()

        # 参考 http://www.madopro.net/entry/bitcoin_chart
        # ローソク足をプロット
        fig = plt.figure(figsize=(18, 9))
        ax = plt.subplot(1, 1, 1)
        candlestick2_ohlc(ax, df_btc["first"], df_btc["max"], df_btc["min"], df_btc["last"], width=0.9, colorup="b", colordown="r")

        # 横軸のセット
        ax.set_xticklabels([(df_btc.index[int(x)] if x < df_btc.shape[0] else x) for x in ax.get_xticks()], rotation=90)
        ax.set_xlim([0, df_btc.shape[0]])
        ax.set_ylabel("Price")
        ax.grid()

        # ローソク足のサイズ調整
        bottom, top = ax.get_ylim()
        ax.set_ylim(bottom - (top - bottom) / 4, top)

        # 出来高を上からプロット
        ax2 = ax.twinx()
        volume_overlay(ax2, df_btc["first"], df_btc["last"], df_btc["size"], width=1, colorup="g", colordown="g")
        ax2.set_xlim([0, df_btc.shape[0]])

        # 出来高のサイズ調整
        ax2.set_ylim([0, df_btc["size"].max() * 4])
        ax2.set_ylabel("Volume")

        plt.show()

    def load_btc_data(self):
        # type: () -> df
        """
        Load hloc btc data.
        :return: hloc btc data frame
        """

        df_btc = pd.read_csv(self.input_file, index_col='datetime')
        self.logger.logger.info('Load btc file: {}'.format(self.input_file))
        return df_btc


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help='file name(including directory path).',
                        action='store',
                        required=True)

    args = parser.parse_args()
    logger = logger.Logger()
    logger.logger.info('START plotchart')

    plot_chart = PlotChart(logger, args.file)
    plot_chart.run()
