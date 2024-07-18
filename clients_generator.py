import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
from faker import Faker

fake = Faker()

# Généreration de 10 000 clients fictifs
data = {
    "ID_client": [i for i in range(1, 10001)],
    "Nom": [fake.last_name() for _ in range(10000)],
    "Prénom": ['"' + fake.first_name() + '"' for _ in range(10000)],  # Encapsuler le prénom avec des guillemets doubles
    "Date_de_naissance": [fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d") for _ in range(10000)],
    "Adresse": [fake.street_address() for _ in range(10000)],
    "Ville": [fake.city() for _ in range(10000)],
    "Code_postal": [fake.zipcode() for _ in range(10000)],
    "Pays": [fake.country() for _ in range(10000)],
    "Numéro_de_téléphone": ['"' + fake.phone_number() + '"' for _ in range(10000)],  # Encapsuler le numéro de téléphone avec des guillemets doubles
    "Adresse_e-mail": ['"' + fake.email() + '"' for _ in range(10000)],  # Encapsuler l'adresse e-mail avec des guillemets doubles
    "Type_de_compte": [np.random.choice(["Courant", "Épargne", "Entreprise"]) for _ in range(10000)],
    "Solde_du_compte": [round(np.random.uniform(100.0, 100000.0), 2) for _ in range(10000)],
    "Date_ouverture_compte": [fake.date_this_decade().strftime("%Y-%m-%d") for _ in range(10000)],
    "Statut_du_compte": [np.random.choice(["Actif", "Inactif", "Fermé"]) for _ in range(10000)],
}

# Création d'un DataFrame Pandas
df = pd.DataFrame(data)

# Convertion du DataFrame en table Arrow
table = pa.Table.from_pandas(df)

#le fichier parquet là
pq.write_table(table, 'clients_banque.parquet')
