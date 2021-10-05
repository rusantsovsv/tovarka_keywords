import os
import datetime
from tqdm.auto import tqdm
import elastic_utils as el


def save_csv(df, path_csv_dir, name_table=None):
    
    """
    Функция для сохранения csv
    
    Args:
        df (pd.DataFrame): датафрейм со статистикой.
    
    Returns:
    
    """
    
    if not os.path.exists(path_csv_dir):
        os.makedirs(path_csv_dir)
    
    if name_table is None:
        name_table=f"table_{datetime.datetime.now().strftime(format='%Y-%m-%d_%H-%M')}.csv"
    
    df.to_csv(f'{path_csv_dir}/{name_table}', index=False)
    
    return


def save_txt(list_keywords, file_name, path_to_main):
    
    """
    Функция для сохранения списка кейвордов под именем ключевого кейворда
    
    Args:
        list_keywords (list): список кейвордов, полученных из БД.
        file_name (str): имя файла - основной кейворд.
        path_to_main (str): путь к рабочей директории.
        
    Returns:
    
    """
    # проверяем, что существует папка txt, если нет - создаем
    if not os.path.exists(f'{path_to_main}/txt'):
        os.makedirs(f'{path_to_main}/txt')
        
    name_file = f'{path_to_main}/txt/{file_name}.txt'
    
    with open(name_file, "w") as text_file:
        print('\n'.join(list_keywords), file=text_file)
        
    return


def query_generate(keyword, max_size):
    
    """
    Функция для создания запроса в зависимости от кейворда
    
    Args:
        keyword (str): ключевое слово.
        max_size (int): максимальное количество кейвордов.
        
    Returns:
        dict: запрос.
    """
    
    # создадим основу для запроса
    query_body = {
                    "size": max_size,
                    "query":{
                      "bool" : {
                          "should" : [
                           ],
                           "minimum_should_match" : 0
                        }
                     }
                }
    
    # разбиваем кейворд на части
    list_terms = keyword.split()
    
    # записываем количество совпадений
    query_body['query']['bool']['minimum_should_match'] = len(list_terms)
    
    # добавляем каждое слово в запрос
    for term in list_terms:
        query_body['query']['bool']['should'].append({"term" : { "tag" : term }})
    
    return query_body


def return_all_kw(keyword, index_tags, login_es, max_size):
    
    """
    Возвращает все найденные кейворды по тегу в wb_tags_prod
    
    Args:
        keyword (str): ключевое слово.
        
    Returns:
        list: список ключевых слов, найденных по этому ключу в базе.
    """
    
    # генерим запрос
    query = query_generate(keyword, max_size)
        
    # получаем результаты
    result = el.universal_query(index_tags, login_es, query)['hits']['hits']
    
    return [keyword['_source']['tag'] for keyword in result]


def keywords_to_txt(df, index_tags, login_es, max_size, path_to_main):
    
    """
    Функция принимает таблицу и из кейвордов создает текстовые файлы с найденными похожими кейвордами
    
    Args:
        df (pd.DataFrame): таблица, содержащая кейворды.
        
    Returns:
        
    """
    
    keywords = df['Ключевое слово'].values
    # для каждого кейворда сохраняем отдельный файл с набором кейвордов
    for kw in tqdm(keywords):
        # получаем все возможные кейворды
        kws_from_elastic = return_all_kw(kw, index_tags, login_es, max_size)
        
        # пишем в файл
        save_txt(kws_from_elastic, kw, path_to_main)