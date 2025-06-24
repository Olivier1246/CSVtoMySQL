#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convertisseur CSV vers MySQL
Parcourt un dossier, trouve le fichier CSV le plus récent et l'importe en base
"""

import os
import csv
import json
import mysql.connector
from datetime import datetime
import logging
from pathlib import Path
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class CSVToMySQLConverter:
    def __init__(self, config_file='config.json'):
        """Initialise le convertisseur avec le fichier de configuration"""
        self.config_file = config_file
        self.config = self.load_config()
        self.connection = None
        
    def load_config(self):
        """Charge la configuration depuis le fichier JSON"""
        if not os.path.exists(self.config_file):
            self.create_default_config()
            
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Configuration chargée depuis {self.config_file}")
            return config
        except Exception as e:
            logger.error(f"Erreur lors du chargement de la configuration: {e}")
            raise
    
    def create_default_config(self):
        """Crée un fichier de configuration par défaut"""
        default_config = {
            "mysql": {
                "host": "localhost",
                "port": 3306,
                "user": "votre_utilisateur",
                "password": "votre_mot_de_passe",
                "database": "votre_base_de_donnees"
            },
            "csv": {
                "folder_path": "./csv_files",
                "table_name": "imported_data",
                "encoding": "utf-8",
                "delimiter": ",",
                "create_table_if_not_exists": true
            }
        }
        
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Fichier de configuration par défaut créé: {self.config_file}")
        logger.warning("Veuillez modifier le fichier de configuration avec vos paramètres")
    
    def connect_to_mysql(self):
        """Établit la connexion à MySQL"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['mysql']['host'],
                port=self.config['mysql']['port'],
                user=self.config['mysql']['user'],
                password=self.config['mysql']['password'],
                database=self.config['mysql']['database']
            )
            logger.info("Connexion à MySQL établie avec succès")
            return True
        except mysql.connector.Error as e:
            logger.error(f"Erreur de connexion à MySQL: {e}")
            return False
    
    def find_latest_csv(self):
        """Trouve le fichier CSV le plus récent dans le dossier spécifié"""
        folder_path = Path(self.config['csv']['folder_path'])
        
        if not folder_path.exists():
            logger.error(f"Le dossier {folder_path} n'existe pas")
            return None
        
        csv_files = list(folder_path.glob('*.csv'))
        
        if not csv_files:
            logger.warning(f"Aucun fichier CSV trouvé dans {folder_path}")
            return None
        
        # Trouve le fichier le plus récent basé sur la date de modification
        latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
        
        modification_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
        logger.info(f"Fichier CSV le plus récent: {latest_file.name} (modifié le {modification_time})")
        
        return latest_file
    
    def analyze_csv_structure(self, csv_file):
        """Analyse la structure du fichier CSV pour créer la table"""
        try:
            with open(csv_file, 'r', encoding=self.config['csv']['encoding']) as f:
                reader = csv.reader(f, delimiter=self.config['csv']['delimiter'])
                headers = next(reader)
                
                # Analyse quelques lignes pour déterminer les types de données
                sample_rows = []
                for i, row in enumerate(reader):
                    if i >= 10:  # Analyse les 10 premières lignes
                        break
                    sample_rows.append(row)
            
            return headers, sample_rows
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse du CSV: {e}")
            return None, None
    
    def guess_column_type(self, values):
        """Devine le type de colonne basé sur les valeurs d'exemple"""
        # Supprime les valeurs vides
        non_empty_values = [v for v in values if v.strip()]
        
        if not non_empty_values:
            return "TEXT"
        
        # Test pour les entiers
        try:
            all(int(v) for v in non_empty_values)
            return "INT"
        except ValueError:
            pass
        
        # Test pour les nombres décimaux
        try:
            all(float(v) for v in non_empty_values)
            return "DECIMAL(10,2)"
        except ValueError:
            pass
        
        # Test pour les dates
        date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']
        for date_format in date_formats:
            try:
                all(datetime.strptime(v, date_format) for v in non_empty_values)
                return "DATETIME" if '%H:%M:%S' in date_format else "DATE"
            except ValueError:
                continue
        
        # Détermine la longueur pour VARCHAR
        max_length = max(len(v) for v in non_empty_values) if non_empty_values else 255
        varchar_length = min(max(max_length, 50), 500)  # Entre 50 et 500 caractères
        
        return f"VARCHAR({varchar_length})"
    
    def create_table(self, table_name, headers, sample_rows):
        """Crée la table MySQL basée sur la structure du CSV"""
        if not self.connection:
            return False
        
        cursor = self.connection.cursor()
        
        try:
            # Analyse les types de colonnes
            columns_def = []
            for i, header in enumerate(headers):
                # Nettoie le nom de la colonne
                clean_header = header.replace(' ', '_').replace('-', '_')
                clean_header = ''.join(c for c in clean_header if c.isalnum() or c == '_')
                
                # Récupère les valeurs de cette colonne
                column_values = [row[i] if i < len(row) else '' for row in sample_rows]
                column_type = self.guess_column_type(column_values)
                
                columns_def.append(f"`{clean_header}` {column_type}")
            
            # Ajoute une colonne ID auto-incrémentée
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(columns_def)},
                `import_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info(f"Table `{table_name}` créée avec succès")
            return True
            
        except mysql.connector.Error as e:
            logger.error(f"Erreur lors de la création de la table: {e}")
            return False
        finally:
            cursor.close()
    
    def import_csv_data(self, csv_file, table_name):
        """Importe les données du CSV dans la table MySQL"""
        if not self.connection:
            return False
        
        cursor = self.connection.cursor()
        
        try:
            with open(csv_file, 'r', encoding=self.config['csv']['encoding']) as f:
                reader = csv.reader(f, delimiter=self.config['csv']['delimiter'])
                headers = next(reader)
                
                # Nettoie les noms des colonnes
                clean_headers = []
                for header in headers:
                    clean_header = header.replace(' ', '_').replace('-', '_')
                    clean_header = ''.join(c for c in clean_header if c.isalnum() or c == '_')
                    clean_headers.append(clean_header)
                
                # Prépare la requête d'insertion
                placeholders = ', '.join(['%s'] * len(headers))
                columns = ', '.join([f"`{h}`" for h in clean_headers])
                insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                
                # Importe les données par lots
                batch_size = 1000
                batch = []
                total_rows = 0
                
                for row in reader:
                    # Assure que la ligne a le bon nombre de colonnes
                    while len(row) < len(headers):
                        row.append('')
                    
                    batch.append(row[:len(headers)])
                    
                    if len(batch) >= batch_size:
                        cursor.executemany(insert_sql, batch)
                        self.connection.commit()
                        total_rows += len(batch)
                        logger.info(f"{total_rows} lignes importées...")
                        batch = []
                
                # Importe le dernier lot
                if batch:
                    cursor.executemany(insert_sql, batch)
                    self.connection.commit()
                    total_rows += len(batch)
                
                logger.info(f"Import terminé: {total_rows} lignes importées dans `{table_name}`")
                return True
                
        except Exception as e:
            logger.error(f"Erreur lors de l'import des données: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def run(self):
        """Exécute le processus complet de conversion"""
        logger.info("Démarrage du processus de conversion CSV vers MySQL")
        
        # Connexion à MySQL
        if not self.connect_to_mysql():
            return False
        
        try:
            # Trouve le fichier CSV le plus récent
            csv_file = self.find_latest_csv()
            if not csv_file:
                return False
            
            # Analyse la structure du CSV
            headers, sample_rows = self.analyze_csv_structure(csv_file)
            if not headers:
                return False
            
            table_name = self.config['csv']['table_name']
            
            # Crée la table si nécessaire
            if self.config['csv']['create_table_if_not_exists']:
                if not self.create_table(table_name, headers, sample_rows):
                    return False
            
            # Importe les données
            if self.import_csv_data(csv_file, table_name):
                logger.info("Conversion terminée avec succès!")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Erreur générale: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()
                logger.info("Connexion MySQL fermée")

def main():
    """Point d'entrée principal"""
    converter = CSVToMySQLConverter()
    
    try:
        success = converter.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Processus interrompu par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()