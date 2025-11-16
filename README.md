# EPSI-M1-DataManagement-TD3

## TD 3 : Validation qualité

Pipeline ETL (Extract, Transform, Load) avec validation de qualité des données utilisant Pandas et Pandera.

---

## 🎯 Objectifs pédagogiques

- Comprendre les principes d'un pipeline ETL
- Maîtriser la validation de données avec Pandera
- Implémenter des transformations de données robustes
- Gérer les erreurs et la qualité des données
- Mettre en place des tests unitaires avec Pytest
- Séparer les données valides des données invalides

---

## 📁 Structure générale du TD

```
TD3/
├── data/
│   ├── input/
│   │   └── users_raw.csv
│   ├── trusted/
│   │   └── dylan_YYYYMMDD_HHMMSS.parquet
│   ├── quarantine/
│   │   └── dylan_YYYYMMDD_HHMMSS.parquet
│   └── alerts/
│       ├── alert_YYYYMMDD_HHMMSS.txt
│       └── alert_YYYYMMDD_HHMMSS.json
├── tests/
│   └── test_etl_functions.py
├── etl_pipeline.py
└── README.md
```

---

## 📝 Description

### Objectif

Ce projet implémente un pipeline ETL complet qui :

1. **Extrait** les données depuis un fichier CSV
2. **Transforme** les données (normalisation, conversion de types)
3. **Valide** la qualité des données avec des règles métier
4. **Charge** les données dans deux destinations :
   - **Trusted** : Données valides au format Parquet
   - **Quarantine** : Données invalides pour analyse

### Fonctionnalités principales

#### 🔄 Extraction
- Lecture de fichiers CSV
- Gestion des erreurs de lecture

#### 🔧 Transformation
- Conversion en minuscules (`username`, `email`)
- Conversion numérique (`user_id`, `age`)
- Conversion de dates (`signup_date`)
- Remplacement des valeurs invalides par `pd.NA` ou `NaT`

#### ✅ Validation
Les règles de validation appliquées :

| Colonne | Type | Contraintes |
|---------|------|-------------|
| `user_id` | Int64 | Unique, Non-null |
| `username` | String | Non-null |
| `email` | String | Format email valide, Non-null |
| `age` | Int64 | Entre -20 et 100, Non-null |
| `signup_date` | Datetime | Non-null |

#### 📊 Séparation des données
- **Données valides** → `data/trusted/`
- **Données invalides** → `data/quarantine/`
- **Alertes** → `data/alerts/` (formats TXT et JSON)

---

## 🛠️ Technologies utilisées

- **Python 3.12+**
- **Pandas** : Manipulation de données
- **Pandera** : Validation de schémas de données
- **PyArrow** : Export Parquet
- **Pytest** : Tests unitaires

---

## 📦 Installation générale

### Prérequis

- Python 3.12 ou supérieur
- pip (gestionnaire de paquets Python)

### Installer les dépendances

```bash
# Cloner le projet
cd TD3

# Créer un environnement virtuel (recommandé)
python3 -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate

# Installer les dépendances
pip install pandas pandera pyarrow pytest
```

### Structure des dépendances

```txt
pandas>=2.0.0
pandera>=0.17.0
pyarrow>=14.0.0
pytest>=7.0.0
```

---

## 🚀 Exécution

### Exécuter le pipeline ETL

```bash
python3 etl_pipeline.py
```

### Sortie attendue (succès)

```
Data extraction successful.
Data transformation successful.
Data Quality Check Passed. Pipeline finished successfully.
```

### Sortie attendue (échec avec données invalides)

```
Data extraction successful.
Data transformation successful.
[Détails des erreurs de validation]
ON VIOLATION
```

### Exécuter les tests unitaires

```bash
# Exécuter tous les tests
pytest

# Exécuter avec détails
pytest -v

# Exécuter un test spécifique
pytest tests/test_etl_functions.py::test_transform_data_valid_input

# Exécuter avec couverture de code
pytest --cov=etl_pipeline
```

---

## 📚 Documentation des fonctions

### `extract_Dataframe_From_CSV(path)`
Extrait les données d'un fichier CSV.

**Paramètres :**
- `path` (str) : Chemin vers le fichier CSV

**Retourne :** DataFrame pandas

---

### `lowercase_Dataframe_Column(dataframe, column)`
Convertit une colonne en minuscules et remplace les chaînes vides par `pd.NA`.

**Paramètres :**
- `dataframe` (pd.DataFrame) : DataFrame source
- `column` (str) : Nom de la colonne à transformer

**Retourne :** DataFrame modifié

---

### `replace_Dataframe_Column_Not_Numeric_To_NULL(dataframe, column)`
Convertit une colonne en numérique (Int64), les valeurs non-numériques deviennent `pd.NA`.

**Paramètres :**
- `dataframe` (pd.DataFrame) : DataFrame source
- `column` (str) : Nom de la colonne à transformer

**Retourne :** DataFrame modifié

---

### `replace_Dataframe_Column_Not_Datetime_To_NULL(dataframe, column)`
Convertit une colonne en datetime, les valeurs invalides deviennent `NaT`.

**Paramètres :**
- `dataframe` (pd.DataFrame) : DataFrame source
- `column` (str) : Nom de la colonne à transformer

**Retourne :** DataFrame modifié

---

### `transform_data(dataframe)`
Applique toutes les transformations sur le DataFrame.

**Transformations appliquées :**
1. Conversion numérique de `user_id` et `age`
2. Conversion en minuscules de `username` et `email`
3. Conversion datetime de `signup_date`

**Retourne :** DataFrame transformé

---

### `validate_data(dataframe)`
Valide les données selon le schéma Pandera défini.

**Retourne :**
- `valid_dataframe` : Données valides
- `failed_dataframe` : Données invalides
- `exception_error` : Détails des erreurs (ou None)

---

## 🎓 Compétences acquises

### Techniques
- ✅ Conception et implémentation d'un pipeline ETL
- ✅ Validation de données avec Pandera
- ✅ Gestion des types nullable (Int64, string)
- ✅ Manipulation avancée de DataFrames Pandas
- ✅ Export au format Parquet
- ✅ Tests unitaires avec Pytest

### Bonnes pratiques
- ✅ Séparation des préoccupations (Extract/Transform/Validate)
- ✅ Gestion des erreurs et exceptions
- ✅ Logging et alertes
- ✅ Tests unitaires complets
- ✅ Documentation du code

### Qualité des données
- ✅ Définition de règles de validation
- ✅ Séparation données valides/invalides
- ✅ Traçabilité des erreurs
- ✅ Gestion des valeurs manquantes

---

## 📊 Exemple de flux de données

```
users_raw.csv
     ↓
[EXTRACTION]
     ↓
DataFrame brut
     ↓
[TRANSFORMATION]
- Normalisation texte
- Conversion types
- Gestion valeurs nulles
     ↓
DataFrame transformé
     ↓
[VALIDATION]
     ↓
   ↙     ↘
VALIDE   INVALIDE
  ↓         ↓
trusted/  quarantine/
```

---

## 🐛 Gestion des erreurs

### Types d'erreurs détectées

1. **Duplicatas** : `user_id` dupliqués
2. **Valeurs manquantes** : Colonnes obligatoires nulles
3. **Formats invalides** : Emails malformés
4. **Valeurs hors limites** : Age < -20 ou > 100
5. **Types incompatibles** : Dates/nombres invalides

### Fichiers d'alerte

Les erreurs génèrent deux fichiers :

**alert_YYYYMMDD_HHMMSS.txt**
```
CRITICAL: ETL Pipeline failed for users_raw.csv. See json for details.
```

**alert_YYYYMMDD_HHMMSS.json**
```json
{
  "DATA": {
    "SERIES_CONTAINS_DUPLICATES": [...],
    "DATAFRAME_CHECK": [...]
  }
}
```

---

## 🧪 Tests unitaires

Le projet contient 13 tests couvrant :

- Transformation en minuscules
- Conversion numérique avec gestion d'erreurs
- Conversion datetime avec gestion d'erreurs
- Transformation complète du DataFrame
- Validation des entrées
- Préservation de la structure des données
- Gestion des cas limites

**Exécution des tests :**
```bash
pytest -v
```

---

## 📄 Licence

Projet libre d'utilisation et de modification — **usage pédagogique EPSI**.

---

## 👨‍💻 Auteur

Dylan LLODRA - M1 Data Management - EPSI

---

## 📞 Support

Pour toute question concernant ce TD :
- Consulter la documentation Pandera : https://pandera.readthedocs.io/
- Consulter la documentation Pandas : https://pandas.pydata.org/docs/