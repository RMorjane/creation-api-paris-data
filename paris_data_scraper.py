import json
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.chrome.options import Options

dict_months = {'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12}

# fonction utilitaire permettant d'instancier le driver chrome       
def get_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# fonction utilitaire permettant de formatter une date à partir d'une chaine de caractère
def get_timestamp(str_date):
    raw_date = str_date.split(' ')
    raw_time = raw_date[3].split(':')
    day = int(raw_date[0])
    month = dict_months[raw_date[1]]
    year = int(raw_date[2])
    hours = int(raw_time[0])
    minutes = int(raw_time[1])
    d = datetime(year,month,day,hours,minutes)
    return d

# classe permettant d'aspirer les données du site paris data
class ParisDataScraper:
    
    def __init__(self):
        self.display = Display(visible=0, size=(1920, 1080))
        self.driver = None
        self.url = 'https://opendata.paris.fr'
        self.list_themes = [] # liste des themes
        self.list_dataset = [] # liste des jeux ce données
        self.list_records = [] # liste des enregistrements
        self.list_keywords = [] # liste des mots clés
        self.list_dataset_keywords = [] # liste des mots clés par jeux de données

    # méthode de la classe permettant de charger le driver chrome        
    def load_driver(self):
        self.display.start()
        self.driver = get_chrome_driver()

    # méthode permettant de lister tous les themes du site           
    def read_themes(self):
        self.driver.get(self.url+'/page/home/')
        soup = BeautifulSoup(self.driver.page_source,'lxml')
        self.list_themes = [
            {
                "theme_name": " ".join(loop_link.find('div').string.split()),
                "theme_link": self.url+loop_link['href']
            }
            for loop_link in soup.find_all('a',{'class': 'box-theme'})
        ]

    # méthode permettant de lister tous les jeux de données du site         
    def read_dataset(self):
        i = 0
        for loop_theme in self.list_themes:
            i += 1
            print('theme link : ',loop_theme['theme_name'])
            self.driver.get(loop_theme['theme_link'])
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source,'lxml')

            for loop_item in soup.find_all('div',{'class': 'ods-catalog-card'}):
                list_metadata = [loop_metadata.text for loop_metadata in loop_item.find_all('span',
                {'class': 'ods-catalog-card__metadata-item-value-text ng-binding ng-scope'})]
                dataset_desc = ''
                try:
                    dataset_desc = " ".join(loop_item.find('p').string.split())
                except:
                    pass
                dataset_api_link = ''
                try:
                    dataset_api_link = self.url+loop_item.find('a',
                    {'class': 'ods-catalog-card__visualization ng-scope'})['href']
                except:
                    pass
                dataset_records_count = 0
                try:
                    dataset_records_count = int(" ".join(loop_item.find('span',
                    {'translate-n': 'value'}).string.split()).split(' éléments')[0].replace(' ',''))
                except:
                    pass

                self.list_dataset.append(
                    {
                        "theme_id": i,
                        "dataset_title": " ".join(loop_item.find('h2').string.split()),
                        "dataset_desc": dataset_desc,
                        "dataset_modified": get_timestamp(loop_item.find('span',
                                            {'class': 'ng-binding ng-scope'}).text),
                        "dataset_producer": list_metadata[0],
                        "dataset_license": list_metadata[1],
                        "dataset_keyword": [" ".join(loop_keyword.split()) 
                                            for loop_keyword in list_metadata[2].split(', ') 
                                            if list_metadata[2]!=''],
                        "dataset_records_count": dataset_records_count,
                        "dataset_api_link": dataset_api_link      
                    }
                )
    
    # récupération de l'api correspondant à l'url du jeux de données
    def get_dataset_api(self,dataset_api_link: str):
        try:
            self.driver.get(dataset_api_link)
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source,'lxml')
            link_api = self.url+soup.find('a',{'class': 'ng-binding'})['href']
            print(link_api)
            response = requests.get(link_api)
            if response.status_code == 200:
                dataset_api = json.loads(response.text)
                json.dumps(dataset_api,indent=2)
                return dataset_api
            else:
                return {}
        except:
            return {}
    
    # méthode de la classe permettant de lister tous les enregistrements des jeux de données du site                              
    def read_records(self):
        i = 0
        for loop_dataset in self.list_dataset:
            i += 1
            if loop_dataset['dataset_records_count']>0:
                dataset_api = self.get_dataset_api(loop_dataset['dataset_api_link'])
                if dataset_api != {}:
                    print(i)
                    for loop_record in dataset_api['records']:
                        self.list_records.append(
                            {
                                "dataset_id": i,
                                "record_timestamp": loop_record.get('record_timestamp',datetime.now()),
                                "record_direction": loop_record['fields'].get('direction','None'),
                                "record_objet_dossier": loop_record['fields'].get('objet_du_dossier','None'),
                                "record_nature_subvention": loop_record['fields'].get('nature_de_la_subvention','None'),
                                "record_collectivite": loop_record['fields'].get('collectivite','None'),
                                "record_secteurs_activite": loop_record['fields'].get('secteurs_d_activites_definies_par_l_association','None'),
                                "record_annee_budgetaire": loop_record['fields'].get('annee_budgetaire','None'),
                                "record_nom_beneficiaire": loop_record['fields'].get('nom_beneficiaire','None'),
                                "record_numero_dossier": loop_record['fields'].get('numero_de_dossier','None'),
                                "record_numero_siret": loop_record['fields'].get('numero_siret','None'),
                                "record_montant_vote": loop_record['fields'].get('montant_vote',0)
                            }
                        )
    
    # méthode de la classe permettant de lister tous les mots clés du site                   
    def read_keywords(self):
        i = 0
        for loop_dataset in self.list_dataset:
            i += 1
            for loop_keyword in loop_dataset['dataset_keyword']:
                if loop_keyword not in self.list_keywords:
                    self.list_keywords.append(loop_keyword)
                self.list_dataset_keywords.append(
                    {
                        "dataset_id": i,
                        "keyword_id": self.list_keywords.index(loop_keyword) + 1
                    }
                ) 