import json
import time
import requests
from pprint import pprint
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.chrome.options import Options

dict_months = {'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12}
        
def get_chrome_driver():
    display = Display(visible=0, size=(1920, 1080))
    display.start()
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

def get_timestamp(str_date):
    raw_date = str_date.split(' ')
    raw_time = raw_date[3].split(':')
    day = int(raw_date[0])
    month = dict_months[raw_date[1]]
    year = int(raw_date[2])
    hours = int(raw_time[0])
    minutes = int(raw_time[1])
    d = datetime(year,month,day,hours,minutes)
    return int(d.timestamp())

api_driver = get_chrome_driver()

def get_api(url):
    api_driver.get(url)
    soup = BeautifulSoup(api_driver.page_source,'lxml')
    link_api = soup.find('a',{'class': 'ng-binding'})['href']
    print(link_api)
    response = requests.get('https://opendata.paris.fr'+link_api)
    if response.status_code == 200:
        json_data = json.loads(response.text)
        json.dumps(json_data,indent=2)
        return json_data
    else:
        return {}

class ParisDataScraper:
    
    def __init__(self):
        self.driver = get_chrome_driver()
        self.url = 'https://opendata.paris.fr'
        self.links = []
           
    def read_links(self):
        self.driver.get(self.url+'/page/home/')
        soup = BeautifulSoup(self.driver.page_source,'lxml')
        self.links = [self.url+loop_links['href']
        for loop_links in soup.find_all('a',{'class': 'box-theme'})]
        
    def read_data(self):
        for loop_links in self.links:
            print('theme link : ',loop_links)
            self.driver.get(loop_links)
            time.sleep(1)
            soup = BeautifulSoup(self.driver.page_source,'lxml')
            
            for loop_item in soup.find_all('div',{'class': 'ods-catalog-card'}):
                
                title_item = loop_item.find('h2').text
                print('title item : ',title_item)
                
                desc_item = loop_item.find('p').text
                print('desc item : ',desc_item)
                
                modified_item = get_timestamp(loop_item.find('span',{'class': 'ng-binding ng-scope'}).text)
                print('modified item : ',modified_item)
            
                list_metadata = [loop_metadata.text for loop_metadata in loop_item.find_all('span',
                {'class': 'ods-catalog-card__metadata-item-value-text ng-binding ng-scope'})]
                print('producer item : ',list_metadata[0])
                print('license item : ',list_metadata[1])
                print('keyword item : ',list_metadata[2])
                
                records_count_item = loop_item.find('span',{'translate-n': 'value'}).text
                print('records count item : ',records_count_item)
                
                api_item = ''
                try:
                    api_item = self.url+loop_item.find('a',
                    {'class': 'ods-catalog-card__visualization ng-scope'})['href']
                    print('api item : ',api_item,'\n')
                except:
                    print('api item : ',api_item,'\n')         

data = ParisDataScraper()
data.read_links()
data.read_data()
data.driver.close()
api_driver.close()