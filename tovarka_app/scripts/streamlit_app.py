"""
Работа через env
"""


import os
import sys
import base64
from pathlib import Path
import streamlit as st
from stqdm import stqdm


PATH_MAIN_FOLDER = str(Path(__file__).parent.absolute().parents[0])
PATH_CSV = f'{PATH_MAIN_FOLDER}/csv'

# путь к модулям с утилитами
sys.path.append(f'{PATH_MAIN_FOLDER}/scripts')

# импортируем парсеры и обработчики
from parsers_utils import *
from keywords_preprocess import *


# задаем глобальные переменные
INDEX_TAGS = os.environ.get('INDEX_TAGS')
LOGIN_ES = os.environ.get('CONNECT_TO_ELASTIC')
MAX_SIZE = int(os.environ.get('MAX_TAGS_SIZE'))
COOKIE = os.environ.get('MAYAK_COOKIE')

st.sidebar.title("Аналитика кейвордов")
st.sidebar.text('В поле ниже нужно добавить кейворды,\nкаждое слово или словосочетание\nна отдельной строке.\nПосле этого нажать "Получить данные"')


# добавим возможность записать кейворды
add_keyword_fields = st.sidebar.text_area(
    'Добавьте кейворды',
    help='Введите ключеные слова и словосочетания каждое на своей строке',
    height=250,
    key='keywords'
)


# обновляем ссылку на скачивание
def download_link(object_to_download, download_filename):
    """
    Генерирует ссылку для скачивания объекта object_to_download.

    object_to_download (str, pd.DataFrame):  Объект для загрузки.
    download_filename (str): имя файла и расширение. Например, mydata.csv, some_txt_output.txt

    Examples:
    download_link(YOUR_DF, 'YOUR_DF.csv', 'Click here to download data!')
    download_link(YOUR_STRING, 'YOUR_STRING.txt', 'Click here to download your text!')

    """
    if isinstance(object_to_download, pd.DataFrame):
        object_to_download = object_to_download.to_csv(index=False)

    # some strings <-> bytes conversions necessary here
    b64 = base64.b64encode(object_to_download.encode()).decode()

    download_link_text = f"Скачать список кейвордов для \*{download_filename.replace('.txt', '')}\*"

    return f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'


def convert_df(df):
    return df.to_csv().encode('utf-8')


def analyze_keywords(keywords):

    # подгружаем данные, добавим текстовое описание
    data_load_state = st.text('Получаю данные с WB и Mayak.bz...')

    list_keywords = str_to_list_kw(st.session_state['keywords'])

    # создаем сессию
    session = requests.Session()

    # подготавливаем датафрейм
    df = pd.DataFrame(columns=['Ключевое слово',
                               'Кол-во товара в списке',
                               'Ср. продажи топ 10 в мес в шт.',
                               'Ср. оборот топ 10 в мес в ₽',
                               'Ср. цена продажи топ 10'])

    # начальный индекс строки
    idx = 0

    # подготавливаем список кейвордов
    prep_kw = generate_kws(list_keywords)

    for kw in stqdm(prep_kw, desc='Проход кейвордов'):
        # получаем общее количество товаров по кейворду и список топ-10
        total, id_list = from_api_wbxsearch(kw, session)

        # идем на маяк и забираем оттуда остальную стату
        data = return_from_mayak(id_list, COOKIE)

        # считаем средние показатели
        mean_price = np.ceil(np.mean([item['price'] for item in data.values() if item['price'] is not None])).astype(
            int)
        mean_sales = np.ceil(
            np.mean([item['sales_mo'] for item in data.values() if item['sales_mo'] is not None])).astype(int)
        mean_turnover = mean_price * mean_sales

        # пишем в таблицу
        df.loc[idx, 'Ключевое слово'] = re.sub('\+', ' ', kw)
        df.loc[idx, 'Кол-во товара в списке'] = total
        df.loc[idx, 'Ср. продажи топ 10 в мес в шт.'] = mean_sales
        df.loc[idx, 'Ср. оборот топ 10 в мес в ₽'] = mean_turnover
        df.loc[idx, 'Ср. цена продажи топ 10'] = mean_price

        # обновляем idx
        idx += 1

    data_load_state.text('Все данные получены!')
    # раскладываем данные в датафрейм

    # записываем датафрейм в состояние сессии
    st.session_state['df'] = df.sort_values(by='Ср. оборот топ 10 в мес в ₽', ascending=False)
    return


# производить вызов функции парсера при нажатии
state_button_get = st.sidebar.button('Получить данные', on_click=analyze_keywords, args=(st.session_state['keywords'], ))

if state_button_get:

    st.dataframe(st.session_state['df'])

    # выводим кнопку скачивания
    csv = convert_df(st.session_state['df'])

    st.download_button(label="Загрузить CSV",
                        data=csv,
                        file_name='result_df.csv',
                        mime='text/csv',)

# делаем кнопки для записи txt
list_keywords = [keyw.replace('+', ' ') for keyw in generate_kws(str_to_list_kw(st.session_state['keywords']))]

st.sidebar.title('Сохранение txt')
st.sidebar.text('Добавьте выше в поле кейворды и нажмите\nCtrl+Enter для обновления вариантов\nсохранения.')


radio_obj = st.sidebar.radio('Выбери кейворд', list_keywords, kwargs=dict())
if radio_obj is not None:
    # отрисовываем кнопку для загрузки
    text_kw = keywords_to_txt(radio_obj, index_tags=INDEX_TAGS, login_es=LOGIN_ES, max_size=MAX_SIZE)

    # только при отжатой кнопке
    if not state_button_get:
        st.markdown(download_link(text_kw, f'{radio_obj}.txt'), unsafe_allow_html=True)
