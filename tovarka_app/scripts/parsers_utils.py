"""
Код парсеров
"""

import re
import json
import time
import urllib
import requests
import numpy as np
import pandas as pd
from tqdm.auto import tqdm
from bs4 import BeautifulSoup as bs
import streamlit as st
from stqdm import stqdm

"""
# определяем переменную для передачи хедера в запросы
headers = {'accept': '*/*',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
"""


def keyword_from_url(url):
    
    """
    Получение кейворда из url
    
    Args:
        url (str): адрес страницы товаров по кейворду.
        
    Returns:
        str: строка кейворда.
    """
    
    # парсим url
    parse_url = urllib.parse.unquote(url)
    
    # сплитим по &
    spl_url = parse_url.split('&')
    
    # жестко отсекаем лишнее
    for q in spl_url:
        if q[:7] == 'search=':
            keyword = q[7:]
            
    return keyword


def generate_urls(list_kw):
    
    """
    Функция для генерации списка urls из переданного списка кейвордов.
    Так как могут быть переданы кейворды, нужна эта проверка.
    
    Args:
        list_kw(list): список кейвордов и url.
        
    Returns:
        list: список кейвордов.
    """
    
    # формируем списко кейвордов
    urls_prepare_list = []

    for kw in list_kw:
        # проверяем, что это не ссылка
        if 'www.wildberries.ru' not in kw:
            urls_prepare_list.append(link_for_kw(kw))
        else:
            urls_prepare_list.append(kw)
    
    return urls_prepare_list


def generate_kws(list_kw):
    
    """
    Функция для генерации списка кейвордов из переданного списка кейвордов.
    Так как могут быть переданы urls, нужна эта проверка.
    
    Args:
        list_kw(list): список кейвордов и url.
        
    Returns:
        list: список кейвордов.
    """
    
    # формируем списко кейвордов
    kw_prepare_list = []

    for kw in list_kw:
        # проверяем, что это не ссылка
        if 'www.wildberries.ru' not in kw:
            kw_prepare_list.append(kw)
        else:
            kw_gen = keyword_from_url(kw)
            kw_prepare_list.append(kw_gen)
    
    return kw_prepare_list    


def link_for_kw(kw):
    
    """ Формирует ссылку на страницу с похожими WB

    Args:
        kw (str): подготовленный кейворд.

    Returns:
        str : ссылка на WB - запрос по кейворду.

    """
    
    return f"https://www.wildberries.ru/catalog/0/search.aspx?search={'%20'.join(kw.split())}&xsearch=true&xsearch=true&sort=popular"


def return_from_mayak(dict_idx, cookie):
    
    """ Получение данных о количестве продаж с mayak.bz

    Args:
        dict_idx (dict): словарь с топ товаров.

    Returns:
        (dict): тот же словарь, но с добавленными продажами в месяц по каждому id.

    """
    for id_name in stqdm(dict_idx.keys(), desc='Продажи с маяка'):
        url = f'https://app.mayak.bz/wb/products/{id_name}'
        #dict_res = {'limit': 200, 'keywords': str(id_name)}
        #payload = json.dumps(dict_res)
        headers = {'authority': 'app.mayak.bz', 
                   #'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                   #'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36',
                   #'accept-language': 'ru,en-US;q=0.9,en;q=0.8',
                   #'cookie': '__ddg1=R8HQqNdpvme15503hORM; __ddg2=RtDoAw3zERwid9B8; remember_user_token=eyJfcmFpbHMiOnsibWVzc2FnZSI6Ilcxc3pNREV5T0Ywc0lpUXlZU1F4TWlScVJsSXVZMFl4UkZKalozTlNiRlJDUjFWV2FteGxJaXdpTVRZeU56RTJPRFV5TkM0NU5ESTVNekEzSWwwPSIsImV4cCI6IjIwMzEtMDctMjRUMjM6MTU6MjQuOTQyWiIsInB1ciI6ImNvb2tpZS5yZW1lbWJlcl91c2VyX3Rva2VuIn19--602a6c517420749f84f8e1bb37dcbf8e1a3a5350; _wb_session=wKGRh8m24%2F%2F2KXc3aLY2DXc20Thbq7Kw1qWyPwMr48aWrhfTgn1JvPGWsLdA4gt7ivqepkOfPLWQS9ipJvIKE1N4zPwsfrEoR1%2BU%2B%2Fpz%2B6PIb86sAnlugewz7aMsML8XVr3kVNU%2BIXUbNJDdu%2BYUk8PtCfWNCpQQAGxvnAbWBpERaiU4gi5fIouDUAQJQaNGWtIC%2BgsTv8OlHtRet%2FafV7QtAq9adIjl9IcLXSjy3qNeQUXYiCFo2ItjrS1J7xw2D072uAxaMneU5NouSrh%2FAuPLrjmB9iDgGZemYLjdS9oel9LsWAoJWVy88UkshiS9LbOsw1Kw9EjGhpgtFnZhs12wwNiKZXUwdjkkf1T0nP5kbvKIeHjiRtvVBDIKdnhXe5ugsSz4ahvcmUMe2lTc7rxO2M1HIeILVpIrTb4FLHjs072vcqIS5Vch%2FHa4kYeMVlwpWZkhrZz1A%2FDqVltgsKWmWLYGU6dhWB%2FtA8uDbFZ8Lg7BlRhs8F07OUmTeB90U510RfN5CoVY5jdxoecFQNVCYdJVI6e8dpm8h15y4ALD1VpE88FOlYuebMQlJDRhA0NfZiuP86BZz%2Bbi7uwIQNHRVuEfxr75l4hhwBgQgpo0VkPZG%2FcGVmeKDf0dGyqhYeq5uoVyRfM0d6Pv%2FGZjcsE1CXuU3kl6c4TK846VWrMgPf0fYai%2BEL2Sd5aPLhYB74Lm%2Byp%2F8Q%3D%3D--PnNdat2cYM2CdD9W--tzwNfSWq%2FbJl9PWRFHtnQA%3D%3D',
                   "cookie": cookie
                   }

        try:
            r = requests.get(url, headers=headers, timeout=20)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print(f'Не получены данные для {id_name}.')
            # добавляем пропуски в словарь
            dict_idx[id_name]['sales_mo'] = None
            continue

        soup = bs(r.content, 'html.parser')
        
        # находим количество продаж
        try:
            divs = soup.find_all('h5', attrs={'class': 'card-title d-md-inline'})[0].text
            sales = int(divs.split('продано')[-1].split('шт.')[0].strip())
            dict_idx[id_name]['sales_mo'] = sales
        except IndexError:
            dict_idx[id_name]['sales_mo'] = None
            continue
        time.sleep(3)
    return dict_idx


def from_api_wbxsearch(kw, session, top=10):
    
    """ Получение id и позиций из api wbxsearch

    Args:
        kw (str): кейворд.
        session (requests.sessions.Session): сессия запроса.
        top (int): количество топов для дальнеших расчетов.

    Returns:
        results_id : словарь с id и позициями.

    """
    
    # получаем фильтры
    # генерим header
    headers_filter = {
          "content-length": "0",
          "x-requested-with": "XMLHttpRequest",
          "referer": f"https://www.wildberries.ru/catalog/0/search.aspx?xparams=search=${urllib.parse.quote(kw, safe='')}&xshard=&page=${0}&search=${urllib.parse.quote(kw, safe='')}&xsearch=true",
        }
    url_filters = "https://www.wildberries.ru/user/get-xinfo-v2"
    xinfo = json.loads(requests.post(url_filters, headers=headers_filter).content)['xinfo']
    
    # получаем запрос из url
    complite_query = f'https://wbxsearch.wildberries.ru/exactmatch/v2/common?query={kw}&xsearch=true&xsearch=true&sort=popular'
    
    # эмулируем открытие страниц в браузере
    while True:
        try:
            request = session.get(complite_query, headers=headers_filter)
            break
        except requests.exceptions.RequestException as e:
            # ждем 10 секунд
            print(f' Ошибка --- {e}')
            time.sleep(10)
            continue
    
    soup_pres = bs(request.content, 'html.parser')
    
    # получаем данные для генерации ссылки
    try:
        dict_params_query = eval(soup_pres.contents[0])
    except TypeError as ex:
        print(ex)
        return None, None
    
    if len(dict_params_query) == 0:
        #print("Пустой словарь с параметрами")
        return None, None
    
    #return dict_params_query
    # генерим ссылку на сраницу по кейворду
    try:
        # генерим ссылку, из которой заберем количество конкуретнов
        query_conc = f"https://wbxcatalog-ru.wildberries.ru/{dict_params_query['shardKey']}/filters?filters={dict_params_query['filters']}&{xinfo}&{dict_params_query['query']}"
        # генерим ссылку, из которой заберем топ-10 id
        query_top_items = f"https://wbxcatalog-ru.wildberries.ru/{dict_params_query['shardKey']}/catalog?{xinfo}&{dict_params_query['query']}&search={urllib.parse.quote(kw, safe='')}"
        #return query_conc, query_top_10 
    except KeyError:
        #print('Ошибка ключа')
        return None, None
    
    # получаем общее количество товаров по кейворду
    while True:
        try:
            request_total = session.get(query_conc, headers=headers_filter)
            break
        except requests.exceptions.RequestException as e:
            # ждем 10 секунд
            print(f' Ошибка --- {e}')
            time.sleep(10)
            continue
    
    # получаем общее количество товаров по кейворду
    try:
        total = json.loads(str(bs(request_total.content, 'html.parser')))['data']['total']
    except (SyntaxError, json.JSONDecodeError):
        total = np.nan
    
    # получаем id товаров с первой страницы
    while True:
        try:
            request_top_items = session.get(query_top_items, headers=headers_filter)
            break
        except requests.exceptions.RequestException as e:
            # ждем 10 секунд
            print(f' Ошибка --- {e}')
            time.sleep(10)
            continue
    
    # получаем список с товарами
    try:
        top_items = json.loads(str(bs(request_top_items.content, 'html.parser')))['data']['products'][:top]
    except (SyntaxError, json.JSONDecodeError):
        #print('Битый ответ')
        return None, None
    
    # убрано ограничение для кейвордов с более 100 конкурентами
    if len(top_items) < 1:
        #print('Ничего не найдено')
        return None, None
    
    else:
        results_id = {}
        for no, product in enumerate(top_items):
            
            # записываем словарь под id продукта
            results_id[product['id']] = {}
            
            # записываем размер скидки
            if 'sale' in product:
                results_id[product['id']]['discount'] = int(product['sale'])
                
            # записываем цену, убираем 2 последних нуля
            if 'salePriceU' in product:
                results_id[product['id']]['price'] = int(str(product['salePriceU'])[:-2])
                
            # добавляем позиции
            results_id[product['id']]['position'] = no + 1
    
    return total, results_id


def str_to_list_kw(str_keywords):
    
    """
    Функция принимает список кейвордов в виде строки и отдает в виде списка
    
    Args:
        str_keywords (str): список кейвордов в виде строки.
        
    Returns:
        list: список кейвордов.
    """
    
    list_kw = [keyword.lower() for keyword in str_keywords.split('\n') if len(keyword) > 0]
    
    return list_kw


def return_from_huntersales(dict_idx, login, password):
    """
    Функция для получения месячных продаж и цен для преданного списка id

    Args:
        login (str): логин к huntersales.
        password (str): пароль к huntersales.
        dict_idx (dict): словарь с артикулами товаров и возможно записанными ценами.

    Returns:
        dict: словарь с ключами id и значениями price и sales.
    """

    # создаем новный объект, проверяем, чтобы id были str
    data_hs = {str(key): value for key, value in dict_idx.items()}

    headers = {'accept': '*/*',
               'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}

    # создаем сессию
    s = requests.Session()

    # создаем переменную с личными данными
    data = {'email': login, 'password': password}
    # посылаем запрос на выполнение входа
    s.post('https://huntersales.ru/api/auth/login', data=data)
    item_data = s.get(f"https://huntersales.ru/api/products/ids?ids={','.join([idx for idx in data_hs.keys()])}", headers=headers)
    #print(item_data.status_code)
    if item_data.status_code == 200:
        # проходим по каждому товару
        for item in json.loads(item_data.text):
            data_hs[str(item['id'])]['sales_mo'] = item['lastMonthSalesAmount']
        # выходим из цикла
    else:
        return None

    # пересоберем и отправим словарь
    return data_hs
