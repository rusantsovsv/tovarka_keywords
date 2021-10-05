import sys
import yaml
from pathlib import Path

PATH_MAIN_FOLDER = str(Path(__file__).parent.absolute().parents[0])
PATH_CSV = f'{PATH_MAIN_FOLDER}/csv'

# путь к модулям с утилитами
sys.path.append(f'{PATH_MAIN_FOLDER}/scripts')

# импортируем парсеры и обработчики
from parsers_utils import *
from keywords_preprocess import *

# подгружаем конфиг
with open(f'{PATH_MAIN_FOLDER}/config.yaml') as file:
    config = yaml.safe_load(file)
    
# задаем глобальные переменные
INDEX_TAGS = config['index_tags']
LOGIN_ES = config['connect_to_elastic']
MAX_SIZE = config['max_size']


keyworws = """

конструктор lego
тапочки
женские черные джинсы
тетрадь с кольцами

"""


if __name__ == '__main__':
    
    # генерим таблицу
    df = analyze_keywords(keyworws)

    # сохраняем таблицу
    save_csv(df, PATH_CSV)
    
    # получаем и сохраняем кейворды
    keywords_to_txt(df, INDEX_TAGS, LOGIN_ES, MAX_SIZE, PATH_MAIN_FOLDER)