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
                user = dotenv.get_key(".env","PARIS_DATA_USERNAME"),
                password = dotenv.get_key(".env","PARIS_DATA_PASSWORD"),
                port = "5432"
            )
            print("Connexion réussie : " + str(self.connection))
            return True
        except (Exception, psycopg2.Error) as error:
            print("Impossible de se connecter au serveur postgres : " + str(error))
            return False
        
db = DBParisData()
db.connect()