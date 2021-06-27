from pprint import pprint
from paris_data_db import DBParisData
from paris_data_scraper import ParisDataScraper

if __name__ == '__main__':
    scraper = ParisDataScraper()
    scraper.load_driver()
    scraper.read_themes()
    scraper.read_dataset()
    scraper.read_keywords()
    #scraper.read_records()
    pprint(scraper.list_dataset_keywords)
    db = DBParisData()
    db.connect()
    #db.save_list_themes(scraper.list_themes)
    #db.save_list_dataset(scraper.list_dataset)
    #db.save_list_records(scraper.list_records)
    #db.save_list_keywords(scraper.list_keywords)
    db.save_list_dataset_keywords(scraper.list_dataset_keywords)
    db.connection.close()
    scraper.driver.close()
    scraper.display.stop()