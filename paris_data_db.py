import dotenv
import dotenv
import psycopg2

dotenv.load_dotenv()

class DBParisData:
    
    def __init__(self):
        self.connection = None
        
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
            with self.connection.cursor() as my_cursor:
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
                );"""
                my_cursor.execute(sql_create_tables)
                self.connection.commit()
                my_cursor.close()
            print("Création des tables réussie")
            return True
        except (Exception, psycopg2.Error) as error:
            print(
                "Impossible de créer les tables au niveau du serveur postgres : " + str(error))
            self.connection.close()
            return False
        
db = DBParisData()
db.connect()
db.create_tables()