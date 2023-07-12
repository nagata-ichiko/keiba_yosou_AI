#ライブラリのインポート
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor,as_completed
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import time
import os
import datetime
import time
import re
from selenium.webdriver.common.by import By

from modules import UrlPaths
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class Results:
    @staticmethod
    def scrape(race_id_list):
        """
        レース結果データをスクレイピングする関数
        Parameters:
        ----------
        race_id_list : list
            レースIDのリスト
        Returns:
        ----------
        race_results_df : pandas.DataFrame
            全レース結果データをまとめてDataFrame型にしたもの
        """
        #race_idをkeyにしてDataFrame型を格納
        scrapeing_results = []
        race_results = {}
        race_urls = []
        # URLのリストを作る
        for race_id in tqdm(race_id_list):
            url = "https://db.netkeiba.com/race/" + race_id
            race_urls.append(url)
        # 並列でスクレイピング
        with ThreadPoolExecutor(10) as executor:
            scrapeing_results = list(executor.map(requests.get, race_urls))
            
        for race_id, race_value in tqdm(zip(race_id_list,scrapeing_results)):
            try:
                html = race_value
                html.encoding = "EUC-JP"
                # メインとなるテーブルデータを取得
                df = pd.read_html(html.text)[0]
                # 列名に半角スペースがあれば除去する
                df = df.rename(columns=lambda x: x.replace(' ', ''))
                # 天候、レースの種類、コースの長さ、馬場の状態、日付をスクレイピング
                soup = BeautifulSoup(html.text, "html.parser")
                texts = (
                    soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
                    + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
                )
                info = re.findall(r'\w+', texts)
                for text in info:
                    if text in ["芝", "ダート"]:
                        df["race_type"] = [text] * len(df)
                    if "障" in text:
                        df["race_type"] = ["障害"] * len(df)
                    if "m" in text:
                        df["course_len"] = [int(re.findall(r"\d+", text)[-1])] * len(df)
                    if text in ["良", "稍重", "重", "不良"]:
                        df["ground_state"] = [text] * len(df)
                    if text in ["曇", "晴", "雨", "小雨", "小雪", "雪"]:
                        df["weather"] = [text] * len(df)
                    if "年" in text:
                        df["date"] = [text] * len(df)
                #馬ID、騎手IDをスクレイピング
                horse_id_list = []
                horse_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                    "a", attrs={"href": re.compile("^/horse")}
                )
                for a in horse_a_list:
                    horse_id = re.findall(r"\d+", a["href"])
                    horse_id_list.append(horse_id[0])
                jockey_id_list = []
                jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all(
                    "a", attrs={"href": re.compile("^/jockey")}
                )
                for a in jockey_a_list:
                    jockey_id = re.findall(r"\d+", a["href"])
                    jockey_id_list.append(jockey_id[0])
                df["horse_id"] = horse_id_list
                df["jockey_id"] = jockey_id_list
                #インデックスをrace_idにする
                df.index = [race_id] * len(df)
                race_results[race_id] = df
            #存在しないrace_idを飛ばす
            except IndexError:
                continue
            except AttributeError: #存在しないrace_idでAttributeErrorになるページもあるので追加
                continue
            #wifiの接続が切れた時などでも途中までのデータを返せるようにする
            except Exception as e:
                print(e)
                break
            #Jupyterで停止ボタンを押した時の対処
            except:
                break
        #pd.DataFrame型にして一つのデータにまとめる
        race_results_df = pd.concat([race_results[key] for key in race_results])
        return race_results_df



#馬の過去成績データを処理するクラス
class HorseResults:
    @staticmethod
    def scrape(horse_id_list):
        """
        馬の過去成績データをスクレイピングする関数

        Parameters:
        ----------
        horse_id_list : list
            馬IDのリスト

        Returns:
        ----------
        horse_results_df : pandas.DataFrame
            全馬の過去成績データをまとめてDataFrame型にしたもの
        """
         #horse_idをkeyにしてDataFrame型を格納
        horse_results = {}
        horse_urls = ['https://db.netkeiba.com/horse/' + id for id in horse_id_list]
        def fetch(url):
            try:
                df = pd.read_html(url)[3]
                #受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
                if df.columns[0]=='受賞歴':
                    df = pd.read_html(url)[4]
                return df
            except IndexError:
                return None
        # 並列でスクレイピング
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = {executor.submit(fetch, url): url for url in horse_urls}

        for future in tqdm(as_completed(future_results), total=len(horse_urls)):
            url = future_results[future]
            horse_id = url.split('/')[-1]  # get horse_id from url
            try:
                df = future.result()  
                df.index = [horse_id] * len(df)
                horse_results[horse_id] = df
            except Exception as e:
                print(e)
                continue

        #pd.DataFrame型にして一つのデータにまとめる        
        horse_results_df = pd.concat([horse_results[key] for key in horse_results])
        return horse_results_df

#血統データを処理するクラス
class Peds:
    @staticmethod
    def scrape(horse_id_list):
        """
        血統データをスクレイピングする関数
        Parameters:
        ----------
        horse_id_list : list
            馬IDのリスト
        Returns:
        ----------
        peds_df : pandas.DataFrame
            全血統データをまとめてDataFrame型にしたもの
        """
        peds_dict = {}
        horse_urls = ['https://db.netkeiba.com/horse/ped/' + id for id in horse_id_list]
      
        def fetch(url):
            try:
                df = pd.read_html(url)[0]

                #重複を削除して1列のSeries型データに直す
                generations = {}
                for i in reversed(range(5)):
                    generations[i] = df[i]
                    df.drop([i], axis=1, inplace=True)
                    df = df.drop_duplicates()
                ped = pd.concat([generations[i] for i in range(5)]).rename(url.split('/')[-1])
                
                return ped.reset_index(drop=True)
            except IndexError:
                return None
        
        # 並列でスクレイピング
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = {executor.submit(fetch, url): url for url in horse_urls}

        for future in tqdm(as_completed(future_results), total=len(horse_urls)):
            url = future_results[future]
            horse_id = url.split('/')[-1]  # get horse_id from url
            try:
                ped = future.result()  
                peds_dict[horse_id] = ped
            except Exception as e:
                print(e)
                continue

        #列名をpeds_0, ..., peds_61にする
        peds_df = pd.concat([peds_dict[key] for key in peds_dict], axis=1).T.add_prefix('peds_')
        return peds_df

class Return:
    @staticmethod
    def scrape(race_id_list):
        """
        払い戻し表データをスクレイピングする関数
        Parameters:
        ----------
        race_id_list : list
            レースIDのリスト
        Returns:
        ----------
        return_tables_df : pandas.DataFrame
            全払い戻し表データをまとめてDataFrame型にしたもの
        """
        return_tables = {}
        race_urls = ['https://db.netkeiba.com/race/' + id for id in race_id_list]

        def fetch(url):
            try:
                #普通にスクレイピングすると複勝やワイドなどが区切られないで繋がってしまう。
                #そのため、改行コードを文字列brに変換して後でsplitする
                f = urlopen(url)
                html = f.read()
                html = html.replace(b'<br />', b'br')
                dfs = pd.read_html(html)

                #dfsの1番目に単勝〜馬連、2番目にワイド〜三連単がある
                df = pd.concat([dfs[1], dfs[2]])
                df.index = [url.split('/')[-1]] * len(df)  # set race_id as index
                return df
            except IndexError:
                return None

        # 並列でスクレイピング
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = {executor.submit(fetch, url): url for url in race_urls}

        for future in tqdm(as_completed(future_results), total=len(race_urls)):
            url = future_results[future]
            race_id = url.split('/')[-1]  # get race_id from url
            try:
                df = future.result()  
                return_tables[race_id] = df
            except Exception as e:
                print(e)
                continue

        #pd.DataFrame型にして一つのデータにまとめる
        return_tables_df = pd.concat([return_tables[key] for key in return_tables])
        return return_tables_df

#　古いデータの更新を行うクラス。
def update_data(old, new):
    """
    Parameters:
    ----------
    old : pandas.DataFrame
        古いデータ
    new : pandas.DataFrame
        新しいデータ
    """

    filtered_old = old[~old.index.isin(new.index)]
    return pd.concat([filtered_old, new])

def scrape_kaisai_date(from_: str, to_: str):
    """
    yyyy-mmの形式でfrom_とto_を指定すると、間のレース開催日一覧が返ってくる関数。
    to_の月は含まないので注意。
    """
    print('getting race date from {} to {}'.format(from_, to_))
    # 間の年月一覧を作成
    date_range = pd.date_range(start=from_, end=to_, freq="M")
    # 開催日一覧を入れるリスト
    kaisai_date_list = []
    for year, month in tqdm(zip(date_range.year, date_range.month), total=len(date_range)):
        # 取得したdate_rangeから、スクレイピング対象urlを作成する。
        # urlは例えば、https://race.netkeiba.com/top/calendar.html?year=2022&month=7 のような構造になっている。
        query = [
            'year=' + str(year),
            'month=' + str(month),
        ]
        url = UrlPaths.CALENDAR_URL + '?' + '&'.join(query)
        html = urlopen(url).read()
        time.sleep(1)
        soup = BeautifulSoup(html, "html.parser")
        a_list = soup.find('table', class_='Calendar_Table').find_all('a')
        for a in a_list:
            kaisai_date_list.append(re.findall('(?<=kaisai_date=)\d+', a['href'])[0])
    return kaisai_date_list

def scrape_race_id_list(kaisai_date_list: list, waiting_time=10):
    """
    開催日をyyyymmddの文字列形式でリストで入れると、レースid一覧が返ってくる関数。
    ChromeDriverは要素を取得し終わらないうちに先に進んでしまうことがあるので、
    要素が見つかるまで(ロードされるまで)の待機時間をwaiting_timeで指定。
    """
    race_id_list = []
    driver = prepare_chrome_driver()
    # 取得し終わらないうちに先に進んでしまうのを防ぐため、暗黙的な待機（デフォルト0秒）
    driver.implicitly_wait(waiting_time)
    max_attempt = 2
    print('getting race_id_list')
    for kaisai_date in tqdm(kaisai_date_list):
        try:
            query = [
                'kaisai_date=' + str(kaisai_date)
            ]
            url = UrlPaths.RACE_LIST_URL + '?' + '&'.join(query)
            print('scraping: {}'.format(url))
            driver.get(url)

            for i in range(1, max_attempt):
                try:
                    a_list = driver.find_element(By.CLASS_NAME, 'RaceList_Box').find_elements(By.TAG_NAME, 'a')
                    break
                except Exception as e:
                    # 取得できない場合は、リトライを実施
                    print(f'error:{e} retry:{i}/{max_attempt} waiting more {waiting_time} seconds')

            for a in a_list:
                race_id = re.findall('(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+',
                    a.get_attribute('href'))
                if len(race_id) > 0:
                    race_id_list.append(race_id[0])
        except Exception as e:
            print(e)
            break

    driver.close()
    driver.quit()
    return race_id_list

def prepare_chrome_driver():
    """
    Chromeのバージョンアップは頻繁に発生し、Webdriverとのバージョン不一致が多発するため、
    ChromeDriverManagerを使用し、自動的にバージョンを一致させる。
    """
    # ヘッドレスモード（ブラウザが立ち上がらない）
    options = Options()
    options.add_argument('--headless')
    options.add_argument("--no-sandbox")
    # Selenium3の場合
    #driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    # Selenium4の場合
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    # 画面サイズをなるべく小さくし、余計な画像などを読み込まないようにする
    driver.set_window_size(50, 50)
    return driver

# 過去に取得したデータを取得の対象外にする
def update_id_list(file_path, new_id_list):
    if os.path.exists(file_path):
        old_data = pd.read_pickle(file_path)
        old_id_list = old_data.index.unique()
        return list(set(new_id_list) - set(old_id_list))
    else:
        return new_id_list

# 過去に取得したデータは保存の対象外にする
def scrape_and_save(scrape_func, file_path, param):
    updated_param = update_id_list(file_path, param)

    if not updated_param:
        print(f"No new data to scrape for {file_path}")
        return

    new_data = scrape_func(updated_param)

    if os.path.exists(file_path):
        old_data = pd.read_pickle(file_path)
        data = pd.concat([old_data, new_data])
    else:
        data = new_data

    data.to_pickle(file_path)

# to_の月は含まないので注意。
date = scrape_kaisai_date(
    from_="2020-01-01", 
    to_="2021-07-01"
    )

# 開催日からレースIDの取得
race_id_list = scrape_race_id_list(date)

# レース結果データの取得
scrape_and_save(Results.scrape, 'data/raw/results.pickle', race_id_list)

# 馬の過去成績データの取得

horse_id_list = pd.read_pickle('data/raw/results.pickle')['horse_id'].unique().tolist()
scrape_and_save(HorseResults.scrape, 'data/raw/horse_results.pickle', horse_id_list)

# 血統データの取得
scrape_and_save(Peds.scrape, 'data/raw/horse_blodd_result.pickle', horse_id_list)

# 払い戻し表データの取得
scrape_and_save(Return.scrape, 'data/raw/return_result.pickle', race_id_list)