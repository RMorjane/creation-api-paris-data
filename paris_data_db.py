import dotenv
import psycopg2

dotenv.load_dotenv()

class DBParisData:
    
    def __init__(self):
        self.connection = None
        self.list_data = []
        self.list_fields = {
            "theme": ["theme_id","theme_name","theme_link"],
            
            "dataset": ["dataset_id","theme_id","dataset_title","dataset_desc","dataset_modified",
                        "dataset_producer","dataset_license","dataset_records_count","dataset_api_link"],
            
            "record": ["record_id","dataset_id","record_timestamp","record_direction",
                       "record_objet_dossier","record_nature_subvention","record_collectivite",
                       "record_secteurs_activite","record_annee_budgetaire","record_nom_beneficiaire",
                       "record_numero_dossier","record_numero_siret","record_montant_vote"],
            
            "keyword": ["keyword_id","keyword_name"],
            
            "dataset_keyword": ["dataset_id","keyword_id"]
        }
        
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host = "localhost",
                database = "postgres",
                user = dotenv.get_key(".env","PARIS_DATA_USERNAME"),
                password = dotenv.get_key(".env","PARIS_DATA_PASSWORD"),
            )
            print("Connexion réussie : " + str(self.connection))
            return True
        except (Exception, psycopg2.Error) as error:
            print("Impossible de se connecter au serveur postgres : " + str(error))
            return False
        
    def create_tables(self):
        try:
            with self.connection.cursor() as tables_cursor:
                sql_create_tables = """
                CREATE TABLE IF NOT EXISTS theme(
                    theme_id SERIAL,
                    theme_name VARCHAR(50),
                    theme_link VARCHAR(200)
                );                
                CREATE TABLE IF NOT EXISTS dataset(
                    dataset_id SERIAL,
                    theme_id INT,
                    dataset_title VARCHAR(200),
                    dataset_desc TEXT,
                    dataset_modified TIMESTAMP,
                    dataset_producer VARCHAR(100),
                    dataset_license VARCHAR(50),
                    dataset_records_count INT,
                    dataset_api_link VARCHAR(200)
                );                
                CREATE TABLE IF NOT EXISTS record(
                    record_id SERIAL,
                    dataset_id INT,
                    record_timestamp TIMESTAMP,
                    record_direction VARCHAR(10),
                    record_objet_dossier TEXT,
                    record_nature_subvention VARCHAR(50),
                    record_collectivite VARCHAR(50),
                    record_secteurs_activite TEXT,
                    record_annee_budgetaire VARCHAR(4),
                    record_nom_beneficiaire VARCHAR(50),
                    record_numero_dossier VARCHAR(10),
                    record_numero_siret VARCHAR(14),
                    record_montant_vote INT
                );                
                CREATE TABLE IF NOT EXISTS keyword(
                    keyword_id SERIAL,
                    keyword_name VARCHAR(50)
                );                
                CREATE TABLE IF NOT EXISTS dataset_keyword(
                    dataset_id INT,
                    keyword_id INT,
                    PRIMARY KEY(dataset_id,keyword_id)
                );
                """
                tables_cursor.execute(sql_create_tables)
                self.connection.commit()
                tables_cursor.close()
            print("Création des tables réussie")
            return True
        except (Exception, psycopg2.Error) as error:
            print("Impossible de créer les tables au niveau du serveur postgres : " + str(error))
            self.connection.close()
            return False
        
    def save_list_themes(self,list_themes: list):
        with self.connection.cursor() as theme_cursor:
            sql_save_themes = "INSERT INTO theme(theme_name,theme_link) VALUES(%s,%s)"
            for loop_theme in list_themes:
                theme_cursor.execute(sql_save_themes,
                    (loop_theme['theme_name'],
                     loop_theme['theme_link']))
            self.connection.commit()
            theme_cursor.close()
            
    def save_list_dataset(self,list_dataset: list):
        with self.connection.cursor() as dataset_cursor:
            sql_save_dataset = """INSERT INTO dataset(theme_id,dataset_title,dataset_desc,
            dataset_modified,dataset_producer,dataset_license,dataset_records_count,dataset_api_link)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            for loop_dataset in list_dataset:
                dataset_cursor.execute(sql_save_dataset,
                    (loop_dataset['theme_id'],
                     loop_dataset['dataset_title'],
                     loop_dataset['dataset_desc'],
                     loop_dataset['dataset_modified'],
                     loop_dataset['dataset_producer'],
                     loop_dataset['dataset_license'],
                     loop_dataset['dataset_records_count'],
                     loop_dataset['dataset_api_link']))
            self.connection.commit()
            dataset_cursor.close()
            
    def save_list_records(self,list_records: list):
        with self.connection.cursor() as record_cursor:
            sql_save_record = """INSERT INTO record(dataset_id,record_timestamp,record_direction,
            record_objet_dossier,record_nature_subvention,record_collectivite,record_secteurs_activite,
            record_annee_budgetaire,record_nom_beneficiaire,record_numero_dossier,record_numero_siret,
            record_montant_vote) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            for loop_record in list_records:
                record_cursor.execute(sql_save_record,
                    (loop_record['dataset_id'],
                     loop_record['record_timestamp'],
                     loop_record['record_direction'],
                     loop_record['record_objet_dossier'],
                     loop_record['record_nature_subvention'],
                     loop_record['record_collectivite'],
                     loop_record['record_secteurs_activite'],
                     loop_record['record_annee_budgetaire'],
                     loop_record['record_nom_beneficiaire'],
                     loop_record['record_numero_dossier'],
                     loop_record['record_numero_siret'],
                     loop_record['record_montant_vote']))
            self.connection.commit()
            record_cursor.close()
            
    def save_list_keywords(self,list_keywords: list):
        with self.connection.cursor() as keyword_cursor:
            sql_save_keywords = "INSERT INTO keyword(keyword_name) VALUES(%s)"
            for loop_keyword in list_keywords:
                keyword_cursor.execute(sql_save_keywords,(loop_keyword,))
            self.connection.commit()
            keyword_cursor.close()
            
    def save_list_dataset_keywords(self,list_dataset_keywords: list):
        with self.connection.cursor() as dataset_keyword_cursor:
            sql_save_dataset_keywords = "INSERT INTO dataset_keyword(dataset_id,keyword_id) VALUES(%s,%s)"
            for loop_dataset_keyword in list_dataset_keywords:
                dataset_keyword_cursor.execute(sql_save_dataset_keywords,
                    (loop_dataset_keyword['dataset_id'],
                     loop_dataset_keyword['keyword_id']))
            self.connection.commit()
            dataset_keyword_cursor.close()
            
    def find_data(self,data: str, conditions: dict={}):
        list_keys = []
        list_values = []
        list_args = ()
        
        for key,value in conditions.items():
            list_keys.append(key)
            list_values.append(value)
        
        with self.connection.cursor() as data_cursor:
            sql_list_data = "SELECT * FROM " + data
            for i in range(len(list_keys)):
                if i==0:
                    sql_list_data += " WHERE "
                else:
                    sql_list_data += " AND "
                if type(list_values[i])==int:
                    sql_list_data += list_keys[i] + "=%s"
                elif type(list_values[i])==str:
                    sql_list_data += list_keys[i] + "='%s'"
                list_args = (list_args + (list_values[i],))
                
            data_cursor.execute(sql_list_data % (list_args))
            self.list_data = []
            for loop_data in data_cursor.fetchall():
                self.list_data.append(
                    {
                        loop_field: loop_data[self.list_fields[data].index(loop_field)]
                        for loop_field in self.list_fields[data]
                   }
                )  
            data_cursor.close()
            
    def find_dataset_keywords(self, list_keywords: list):
       with self.connection.cursor() as dk_cursor:
            sql_list_dk = """
            SELECT
                theme_id,
                dataset_keyword.dataset_id,
                dataset_title,
                dataset_desc,
                dataset_modified,
                dataset_producer,
                dataset_license,
                dataset_records_count,
                dataset_api_link
            FROM
                dataset,
                keyword,
                dataset_keyword
            WHERE
                dataset_keyword.dataset_id = dataset.dataset_id
            AND
                dataset_keyword.keyword_id = keyword.keyword_id
            AND
                keyword_name IN (
            """
            for i in range(len(list_keywords)):
                sql_list_dk += "'"+list_keywords[i]+"'"
                if i < len(list_keywords) - 1:
                    sql_list_dk += ","
                else:
                    sql_list_dk += ")"
            dk_cursor.execute(sql_list_dk)
            self.list_data = []
            
            for loop_data in dk_cursor.fetchall():
                self.list_data.append(
                    {
                        loop_field: loop_data[self.list_fields["dataset"].index(loop_field)] 
                        for loop_field in self.list_fields["dataset"]
                    }
                )
            dk_cursor.close()
            
    def find_dataset_theme(self, theme: str):
           with self.connection.cursor() as dt_cursor:
            sql_list_dt ="""
            SELECT * FROM dataset 
            WHERE theme_id IN (SELECT theme_id FROM theme WHERE theme_name='%s')"""
            dt_cursor.execute(sql_list_dt % (theme,))
            self.list_data = []
            
            for loop_data in dt_cursor.fetchall():
                self.list_data.append(
                    {
                        loop_field: loop_data[self.list_fields["dataset"].index(loop_field)] 
                        for loop_field in self.list_fields["dataset"]
                    }
                )
            dt_cursor.close()               