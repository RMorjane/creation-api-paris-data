from paris_data_db import DBParisData

class ParisDataApi:
    
    def __init__(self):
        self.db = DBParisData()
        self.list_themes = []
        self.list_dataset = []
        self.list_keywords = []
        self.list_records = []
        self.list_dataset_keywords = []
        self.list_dataset_theme = []
        
    def read_list_themes(self):
        if not self.db.connection: self.db.connect()
        self.db.find_data("theme")
        self.list_themes = self.db.list_data
        
    def read_list_dataset(self, query: dict = {}):
        if not self.db.connection: self.db.connect()
        self.db.find_data("dataset",query)
        self.list_dataset = self.db.list_data
        
    def read_list_keywords(self):
        if not self.db.connection: self.db.connect()
        self.db.find_data("keyword")
        self.list_keywords = self.db.list_data
        
    def read_list_records(self, query: dict = {}):
        if not self.db.connection: self.db.connect()
        self.db.find_data("record",query)
        self.list_records = self.db.list_data
        
    def read_dataset_keywords(self, list_keywords: list):
        if not self.db.connection: self.db.connect()
        self.db.find_dataset_keywords(list_keywords)
        self.list_dataset_keywords = self.db.list_data
        
    def read_dataset_theme(self, theme: str):
        if not self.db.connection: self.db.connect()
        self.db.find_dataset_theme(theme)
        self.list_dataset_theme = self.db.list_data