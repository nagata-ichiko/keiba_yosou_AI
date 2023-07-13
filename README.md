# keiba_yosou_AI

競馬予想AIに使用する学習データをスクレイピングするリポジトリです。

## 環境構築

1. Python をインストール
2. 以下コマンドを実行し、必要なパッケージをインストールする。

```
pip install -r requirements.txt
```

## スクレイピングの実行

keiba_data_scraping.py を実行する。

情報を取得する期間は以下で変更します。

```
# to_の月は含まないので注意。
date = scrape_kaisai_date(
from_="2018-01-01",
to_="2023-08-01"
)
```

## 高速スクレイピングの実行

keiba_data_scraping_Thread.py を実行する

## スクレイピング結果 CSV 出力

keiba_data_view.py を実行する

# 参考

https://zenn.dev/dijzpeb/books/848d4d8e47001193f3fb
