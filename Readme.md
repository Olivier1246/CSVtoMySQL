Fonctionnalités du programme
Ce programme Python offre les fonctionnalités suivantes :
📁 Gestion des fichiers CSV

Parcourt automatiquement le dossier spécifié
Identifie et sélectionne le fichier CSV le plus récent (basé sur la date de modification)
Support de différents encodages et délimiteurs

🔧 Configuration flexible

Fichier de configuration JSON pour tous les paramètres
Paramètres de connexion MySQL sécurisés
Configuration du chemin des fichiers CSV
Options personnalisables (encodage, délimiteur, nom de table)

🗄️ Gestion de base de données

Connexion automatique à MySQL
Création automatique de table basée sur la structure du CSV
Détection intelligente des types de colonnes (INT, DECIMAL, VARCHAR, DATE, DATETIME)
Import par lots pour de meilleures performances

📊 Fonctionnalités avancées

Logging détaillé dans fichier et console
Gestion d'erreurs robuste
Validation des données
Ajout automatique d'une colonne ID et timestamp d'import

Installation et utilisation

Installer les dépendances :

bash
pip install -r requirements.txt

Modifier la configuration :

Éditer le fichier config.json avec vos paramètres MySQL
Spécifier le chemin vers vos fichiers CSV
Ajuster les autres paramètres selon vos besoins


Exécuter le programme :

bash
python csv_to_mysql.py
Configuration détaillée

mysql : Paramètres de connexion à la base de données
csv.folder_path : Chemin vers le dossier contenant les fichiers CSV
csv.table_name : Nom de la table MySQL de destination
csv.encoding : Encodage des fichiers CSV (utf-8, iso-8859-1, etc.)
csv.delimiter : Délimiteur utilisé dans les CSV (virgule, point-virgule, etc.)
csv.create_table_if_not_exists : Création automatique de la table

Le programme génère des logs détaillés dans le fichier csv_import.log et affiche le progrès en temps réel.