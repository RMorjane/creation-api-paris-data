from pprint import pprint
from paris_data_api import ParisDataApi
    
if __name__ == '__main__':
    
    api = ParisDataApi()
    
    api.read_list_themes()
    pprint(api.list_themes)
    
    api.read_list_dataset()
    pprint(api.list_dataset)
    
    api.read_list_records()
    pprint(api.list_records)
    
    api.read_list_keywords()
    pprint(api.list_keywords)
    
    api.read_dataset_keywords(["loisirs"])
    pprint(api.list_dataset_keywords)