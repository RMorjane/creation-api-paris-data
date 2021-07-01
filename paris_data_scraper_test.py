import unittest
from paris_data_scraper import ParisDataScraper, get_chrome_driver, get_timestamp

scraper = ParisDataScraper()

class TestParisDataScraper(unittest.TestCase):
    
    def test_load_driver(self):
        scraper.load_driver()
        self.assertIsNotNone(scraper.driver)
        
    def test_read_themes(self):
        scraper.read_themes()
        self.assertFalse(len(scraper.list_themes)==0)
        
    def test_read_dataset(self):
        scraper.read_dataset()
        self.assertFalse(len(scraper.list_dataset))
        
if __name__ == '__main__':
    unittest.main()