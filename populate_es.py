import json

import requests
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup

gouv_api = 'https://base-donnees-publique.medicaments.gouv.fr/extrait.php'

titles = ['drug_id', 'drug_name', 'drug_type', 'drug_administration', 'authorization_status', 'authorization_type', 'commercial_status', 'date_of_circulation']

def create_index(es_object, index_name):
    created = False

    settings = {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
    mappings = {
        "properties": {
            "drug_id": {
                "type": "integer"
            },
            "drug_name": {
                "type": "text"
            },
            "drug_type": {
                "type": "text"
            },
            "drug_administration": {
                "type": "text"
            },
            "authorization_status": {
                "type": "text"
            },
            "authorization_type": {
                "type": "text"
            },
            "commercial_status": {
                "type": "text"
            },
            "date_of_circulation": {
                "type": "date",
                "format": "dd/MM/yyyy"
            },
            "price_with_honorary": {
                "type": "float"
            },
            "repayment_rate": {
                "type": "integer"
            },
        }
    }

    try:
        if es_object.indices.exists(index=index_name):
            print('deleting index:', index_name)
            es_object.indices.delete(index=index_name)
            
        es_object.indices.create(index=index_name, settings=settings, mappings=mappings)
        print('index ', index_name, ' created successfully')
        created = True
    except Exception as ex:
        print(create_index.__name__, str(ex))
    finally:
        return created

def connect_elasticsearch():
    _es = Elasticsearch('http://localhost:9200')
    if not _es.ping():
        raise RuntimeError('elasticsearch ping failed')

    return _es

def recover_drug_data(line: str):
    l = list(filter(None, line.split('\t')))

    price_with_honorary = 0
    repayment_rate = 0

    r = requests.get(gouv_api + '?specid=' + l[0])
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        info_cnam = soup.find('span', class_='infosCnam')
        if info_cnam is not None:
            info_cnam = info_cnam.text.replace(u'\xa0', u' ')
            info_cnam = info_cnam.rstrip(' %').split(' ')
            price_with_honorary = float(info_cnam[4].replace(',','.'))
            repayment_rate = int(info_cnam[-1])
    
    drug_dict = dict(zip(titles, l))
    drug_dict['price_with_honorary'] = price_with_honorary
    drug_dict['repayment_rate'] = repayment_rate
    print('recovered drug: ', drug_dict)

    return json.dumps(drug_dict)

if __name__ == '__main__':
    url = 'http://prod-bdm.ansm.integra.fr/telechargement.php?fichier=CIS_bdpm.txt'
    index_name = 'drugs'

    r = requests.get(url)
    if r.status_code == 200:
        es = connect_elasticsearch()
        
        if create_index(es, index_name):
            actions = [
                {
                    '_index': index_name,
                    '_source': recover_drug_data(record)
                } for record in r.text.splitlines()
            ]
            try:
                helpers.parallel_bulk(es, actions)
            except helpers.BulkIndexError as exception:
                print(exception)  
            