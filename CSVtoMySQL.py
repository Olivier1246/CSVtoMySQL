import mysql.connector
import pandas as pd
import hashlib
import os
import json
from datetime import datetime
import logging

class CSVtoMySQL:
    def __init__(self, config_file='config.json'):
        """
        Initialise la connexion à MySQL en utilisant un fichier de configuration JSON
        """
        self.config = self.load_config(config_file)
        self.connection = None
        self.cursor = None
        
        # Configuration du logging
        log_config = self.config.get('logging', {})
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_config.get('file', 'csv_mysql.log')),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_config(self, config_file):
        """
        Charge la configuration depuis un fichier JSON
        """
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            self.create_default_config(config_file)
            raise FileNotFoundError(f"Fichier de configuration '{config_file}' non trouvé. Un fichier par défaut a été créé.")
        except json.JSONDecodeError as e:
            raise ValueError(f"Erreur dans le fichier JSON: {e}")
    
    def create_default_config(self, config_file):
        """
        Crée un fichier de configuration par défaut
        """
        default_config = {
            "database": {
                "host": "localhost",
                "user": "root",
                "password": "",
                "database": "test",
                "charset": "utf8mb4"
            },
            "csv": {
                "encoding": "utf-8",
                "separator": ",",
                "default_table_name": "csv_data"
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(levelname)s - %(message)s",
                "file": "csv_mysql.log"
            },
            "monitoring": {
                "check_interval": 60,
                "auto_create_table": True
            },
            "data_types": {
                "varchar_length": 255,
                "decimal_precision": "10,2"
            }
        }
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)
    
    def connect(self):
        """
        Établit la connexion à MySQL
        """
        try:
            db_config = self.config['database']
            self.connection = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
                charset=db_config.get('charset', 'utf8mb4')
            )
            self.cursor = self.connection.cursor()
            self.logger.info("Connexion à MySQL établie avec succès")
            return True
        except mysql.connector.Error as err:
            self.logger.error(f"Erreur de connexion MySQL: {err}")
            return False
        except KeyError as e:
            self.logger.error(f"Configuration manquante: {e}")
            return False
    
    def disconnect(self):
        """
        Ferme la connexion à MySQL
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Connexion MySQL fermée")
    
    def create_table_from_csv(self, csv_file, table_name):
        """
        Crée une table MySQL basée sur la structure du CSV
        """
        try:
            # Configuration CSV
            csv_config = self.config.get('csv', {})
            data_types_config = self.config.get('data_types', {})
            
            # Lire le CSV pour analyser la structure
            df = pd.read_csv(
                csv_file,
                encoding=csv_config.get('encoding', 'utf-8'),
                sep=csv_config.get('separator', ',')
            )
            
            # Construire la requête CREATE TABLE
            columns = []
            varchar_length = data_types_config.get('varchar_length', 255)
            decimal_precision = data_types_config.get('decimal_precision', '10,2')
            
            for col in df.columns:
                # Déterminer le type de données
                if df[col].dtype == 'int64':
                    col_type = 'INT'
                elif df[col].dtype == 'float64':
                    col_type = f'DECIMAL({decimal_precision})'
                elif df[col].dtype == 'bool':
                    col_type = 'BOOLEAN'
                else:
                    col_type = f'VARCHAR({varchar_length})'
                
                columns.append(f"`{col}` {col_type}")
            
            # Ajouter les colonnes système
            columns.append("`row_hash` VARCHAR(64) UNIQUE")
            columns.append("`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            create_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(columns)}
            )
            """
            
            self.cursor.execute(create_query)
            self.connection.commit()
            self.logger.info(f"Table '{table_name}' créée avec succès")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la création de la table: {e}")
            raise
    
    def generate_row_hash(self, row):
        """
        Génère un hash unique pour une ligne de données
        """
        row_string = '|'.join(str(value) for value in row.values)
        return hashlib.md5(row_string.encode()).hexdigest()
    
    def get_existing_hashes(self, table_name):
        """
        Récupère tous les hashes existants dans la table
        """
        try:
            query = f"SELECT row_hash FROM `{table_name}`"
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return set(row[0] for row in results)
        except mysql.connector.Error as err:
            if "doesn't exist" in str(err):
                return set()
            else:
                raise
    
    def import_csv_initial(self, csv_file, table_name=None):
        """
        Import initial complet du CSV vers MySQL
        """
        try:
            # Utiliser le nom de table par défaut si non spécifié
            if table_name is None:
                table_name = self.config.get('csv', {}).get('default_table_name', 'csv_data')
            
            # Créer la table si configuré pour le faire
            if self.config.get('monitoring', {}).get('auto_create_table', True):
                self.create_table_from_csv(csv_file, table_name)
            
            # Configuration CSV
            csv_config = self.config.get('csv', {})
            
            # Lire le CSV
            df = pd.read_csv(
                csv_file,
                encoding=csv_config.get('encoding', 'utf-8'),
                sep=csv_config.get('separator', ',')
            )
            
            # Préparer les données avec hash
            rows_inserted = 0
            for index, row in df.iterrows():
                row_hash = self.generate_row_hash(row)
                
                # Préparer les valeurs pour l'insertion
                values = list(row.values) + [row_hash]
                placeholders = ', '.join(['%s'] * len(values))
                columns = list(df.columns) + ['row_hash']
                columns_str = ', '.join([f"`{col}`" for col in columns])
                
                insert_query = f"""
                INSERT IGNORE INTO `{table_name}` ({columns_str})
                VALUES ({placeholders})
                """
                
                try:
                    self.cursor.execute(insert_query, values)
                    if self.cursor.rowcount > 0:
                        rows_inserted += 1
                except mysql.connector.Error as err:
                    self.logger.warning(f"Erreur lors de l'insertion de la ligne {index}: {err}")
            
            self.connection.commit()
            self.logger.info(f"Import initial terminé: {rows_inserted} lignes insérées dans '{table_name}'")
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'import initial: {e}")
            raise
    
    def append_new_rows(self, csv_file, table_name=None):
        """
        Ajoute uniquement les nouvelles lignes du CSV qui ne sont pas déjà dans la base
        """
        try:
            # Utiliser le nom de table par défaut si non spécifié
            if table_name is None:
                table_name = self.config.get('csv', {}).get('default_table_name', 'csv_data')
            
            # Vérifier si la table existe
            self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not self.cursor.fetchone():
                self.logger.info(f"Table '{table_name}' n'existe pas, import initial en cours...")
                return self.import_csv_initial(csv_file, table_name)
            
            # Récupérer les hashes existants
            existing_hashes = self.get_existing_hashes(table_name)
            self.logger.info(f"Nombre de lignes existantes: {len(existing_hashes)}")
            
            # Configuration CSV
            csv_config = self.config.get('csv', {})
            
            # Lire le CSV
            df = pd.read_csv(
                csv_file,
                encoding=csv_config.get('encoding', 'utf-8'),
                sep=csv_config.get('separator', ',')
            )
            
            # Identifier les nouvelles lignes
            new_rows = []
            for index, row in df.iterrows():
                row_hash = self.generate_row_hash(row)
                if row_hash not in existing_hashes:
                    new_rows.append((row, row_hash))
            
            if not new_rows:
                self.logger.info("Aucune nouvelle ligne détectée")
                return 0
            
            # Insérer les nouvelles lignes
            rows_inserted = 0
            for row, row_hash in new_rows:
                values = list(row.values) + [row_hash]
                placeholders = ', '.join(['%s'] * len(values))
                columns = list(df.columns) + ['row_hash']
                columns_str = ', '.join([f"`{col}`" for col in columns])
                
                insert_query = f"""
                INSERT IGNORE INTO `{table_name}` ({columns_str})
                VALUES ({placeholders})
                """
                
                try:
                    self.cursor.execute(insert_query, values)
                    if self.cursor.rowcount > 0:
                        rows_inserted += 1
                except mysql.connector.Error as err:
                    self.logger.warning(f"Erreur lors de l'insertion d'une nouvelle ligne: {err}")
            
            self.connection.commit()
            self.logger.info(f"Append terminé: {rows_inserted} nouvelles lignes ajoutées")
            return rows_inserted
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'append: {e}")
            raise
    
    def get_table_stats(self, table_name=None):
        """
        Retourne les statistiques de la table
        """
        try:
            if table_name is None:
                table_name = self.config.get('csv', {}).get('default_table_name', 'csv_data')
            
            # Nombre total de lignes
            self.cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            total_rows = self.cursor.fetchone()[0]
            
            # Date de la dernière insertion
            self.cursor.execute(f"SELECT MAX(created_at) FROM `{table_name}`")
            last_insert = self.cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'total_rows': total_rows,
                'last_insert': last_insert
            }
        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des stats: {e}")
            return None
    
    def monitor_csv_and_sync(self, csv_file, table_name=None):
        """
        Surveille le fichier CSV et synchronise automatiquement
        """
        import time
        
        if table_name is None:
            table_name = self.config.get('csv', {}).get('default_table_name', 'csv_data')
        
        check_interval = self.config.get('monitoring', {}).get('check_interval', 60)
        
        last_modified = 0
        self.logger.info(f"Surveillance du fichier {csv_file} démarrée (intervalle: {check_interval}s)")
        
        try:
            while True:
                if os.path.exists(csv_file):
                    current_modified = os.path.getmtime(csv_file)
                    
                    if current_modified > last_modified:
                        self.logger.info("Modification détectée dans le fichier CSV")
                        new_rows = self.append_new_rows(csv_file, table_name)
                        
                        if new_rows > 0:
                            stats = self.get_table_stats(table_name)
                            self.logger.info(f"Synchronisation terminée. Total: {stats['total_rows']} lignes")
                        
                        last_modified = current_modified
                
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            self.logger.info("Surveillance interrompue par l'utilisateur")
        except Exception as e:
            self.logger.error(f"Erreur lors de la surveillance: {e}")


# Exemple d'utilisation
if __name__ == "__main__":
    try:
        # Initialiser avec le fichier de configuration
        csv_mysql = CSVtoMySQL('config.json')
        
        if csv_mysql.connect():
            try:
                # Fichier CSV (peut être configuré dans le JSON aussi)
                csv_file = 'data.csv'
                
                # Import initial ou ajout de nouvelles lignes
                print("=== Import/Append des données CSV ===")
                new_rows = csv_mysql.append_new_rows(csv_file)
                print(f"Nombre de nouvelles lignes ajoutées: {new_rows}")
                
                # Afficher les statistiques
                stats = csv_mysql.get_table_stats()
                if stats:
                    print(f"Table: {stats['table_name']}")
                    print(f"Total de lignes dans la table: {stats['total_rows']}")
                    print(f"Dernière insertion: {stats['last_insert']}")
                
                # Optionnel: surveillance continue
                # csv_mysql.monitor_csv_and_sync(csv_file)
                
            finally:
                csv_mysql.disconnect()
        else:
            print("Impossible de se connecter à MySQL")
            
    except FileNotFoundError as e:
        print(f"Erreur: {e}")
        print("Veuillez configurer le fichier config.json créé automatiquement.")
    except Exception as e:
        print(f"Erreur: {e}")