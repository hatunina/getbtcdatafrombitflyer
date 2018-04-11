# getbtcdatafrombitflyer
bitflyerからデータ取得開始日と終了日を指定してBTCデータを保存するスクリプトです。 <br>

# Requirements
`pip install pandas` <br>
`pip install progressbar2` <br>

# Usage

プロジェクト直下に`data`ディレクトリと`log`ディレクトリを作成してください。 <br>
それぞれ取得したデータとログが出力されます。 <br>

下記コマンドで実行します。 <br>

`python src/getbtc.py -s 2018-01-01-00:00:00 -f 2018-04-11-23:14:00` <br>

`-d`でデータ取得開始日を指定します。 <br>
`-f`はデータ取得終了日を指定できます。 <br>

上記のコマンドを実行することで2018年に入った瞬間から2018年4月11日23時14分までのデータを取得することができます。 <br>
終了日の方は「~分」の部分は含みません。 <br>
`-f 2018-04-11-23:14:00`という引数は23時13分台のデータが最後になります. <br>

また、終了日の引数`-f`は省略することが可能です。 <br>
省略した場合は開始日から実行時の日付（時間）までのデータを取得します。 <br>
例えば、<br>
 <br>
`python src/getbtc.py -s 2018-01-01-00:00:00` <br>
<br>
とすることで2018年に入ってから現在までのデータを取得することができます（４月１０日時点で約1.4GBほど）。 <br>
 <br>
`YYYY-MM-DD-HH-MM-SS`の形式ですが秒はどんな数字を入れても00として変換されます。 <br>
 <br>
一番古い日付で`2015-06-24 05:58:00`なので、これ以前を渡すとエラーが出ます。 <br>
 <br>
出力結果のファイルは一つのファイルで50万行約50MBに設定してあります。 <br>
必要に応じて変更してください。 <br>
