import time
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException


def get_data_from_elastic(login, query, func='search'):
    
    """Запрос в elasticsearch, но если данные не приходят - ждет 5 секунд и запрашивает их снова.

    Args:
        login (str): логин с адресом для доступа к БД.
        query (dict): запрос к базе.
        func (str): определяет тип запроса - поиск или подсчет количества документов.

    Returns:
        list: возвращает list с ответом от базы. В списке json.

    """
    
    while True:
        es = Elasticsearch(login, timeout=180)
        try:
            if func == 'count':
                count_query = {}
                count_query['index'] = query['index']
                count_query['body'] = {key: value for key, value in query['body'].items() if key not in ['size', 'sort']}
                #print(count_query)
                elastic_answ = es.count(**count_query)
            else:
                elastic_answ = es.search(**query)
            return elastic_answ
        except ElasticsearchException as ex:
            # ждем 5 секунд и начинаем сначала
            print(f'Ошибка БД {ex}')
            time.sleep(5)
            continue

            
def universal_query(index, login, query, func='search'):
    
    """
    Запрос на получение данных по переданному запросу
    
    Args:
        index (str): индекс с характеристикой или именем характеристики.
        login (str): доступ к базе.
        query (dict): запрос к базе.
        func (str): search - поиск документов, count - возврат количества документов в индексе.
        
    Returns:
        list: список с результатами выдачи базы.
    """
    
    body = dict(index=index, body=query)

    elastic_answ = get_data_from_elastic(login, body, func=func)
    
    return elastic_answ