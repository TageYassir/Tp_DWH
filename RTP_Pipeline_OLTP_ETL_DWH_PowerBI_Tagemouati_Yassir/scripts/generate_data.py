import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialisation
fake = Faker('fr_FR')
random.seed(42)
Faker.seed(42)

print("Génération des données en cours...")

# ========================================
# GÉNÉRATION DES CLIENTS (10 000)
# ========================================
print("Génération de 10 000 clients...")

villes = ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 'Nantes',
          'Strasbourg', 'Montpellier', 'Bordeaux', 'Lille', 'Rennes', 'Reims']

clients_data = []
for i in range(1, 10001):
    clients_data.append({
        'id_client': i,
        'nom': fake.last_name(),
        'prenom': fake.first_name(),
        'email': f"client{i}@{fake.free_email_domain()}",
        'ville': random.choice(villes),
        'date_inscription': fake.date_between(start_date='-3y', end_date='today')
    })

df_clients = pd.DataFrame(clients_data)
df_clients.to_csv(f'Put your own link in here like I did in the report!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!', index=False, encoding='utf-8')
print(f"Fichier clients.csv créé : {len(df_clients)} lignes")

# ========================================
# GÉNÉRATION DES PRODUITS (500)
# ========================================
print("Génération de 500 produits...")

categories = {
    'Ordinateurs': ['MacBook Pro', 'Dell XPS', 'HP Pavilion', 'Lenovo ThinkPad', 'Asus VivoBook'],
    'Téléphones': ['iPhone 14', 'Samsung Galaxy S23', 'Google Pixel 7', 'OnePlus 11', 'Xiaomi 13'],
    'Tablettes': ['iPad Pro', 'Samsung Galaxy Tab', 'Microsoft Surface', 'Lenovo Tab', 'Huawei MatePad'],
    'Accessoires': ['Souris Logitech', 'Clavier mécanique', 'Casque Bose', 'Webcam HD', 'Hub USB-C'],
    'Montres': ['Apple Watch', 'Samsung Galaxy Watch', 'Garmin Forerunner', 'Fitbit Sense', 'Huawei Watch']
}

produits_data = []
id_produit = 1
for categorie, noms_base in categories.items():
    for _ in range(100):  # 100 produits par catégorie
        nom_base = random.choice(noms_base)
        produits_data.append({
            'id_produit': id_produit,
            'nom_produit': f"{nom_base} {random.choice(['Pro', 'Plus', 'Ultra', 'Standard', 'Lite'])}",
            'categorie': categorie,
            'prix_unitaire': round(random.uniform(50, 2000), 2)
        })
        id_produit += 1

df_produits = pd.DataFrame(produits_data)
df_produits.to_csv('produits.csv', index=False, encoding='utf-8')
print(f"Fichier produits.csv créé : {len(df_produits)} lignes")

# ========================================
# GÉNÉRATION DES COMMANDES (20 000)
# ========================================
print("Génération de 20 000 commandes...")

commandes_data = []
start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 12, 31)

for i in range(1, 20001):
    random_days = random.randint(0, (end_date - start_date).days)
    date_commande = start_date + timedelta(days=random_days)

    commandes_data.append({
        'id_commande': i,
        'id_client': random.randint(1, 10000),
        'date_commande': date_commande.strftime('%Y-%m-%d'),
        'montant_total': 0  # Sera calculé plus tard
    })

df_commandes = pd.DataFrame(commandes_data)

# ========================================
# GÉNÉRATION DES LIGNES DE COMMANDES (100 000)
# ========================================
print("Génération de 100 000 lignes de commandes...")

lignes_data = []
montants_par_commande = {}

for i in range(1, 100001):
    id_commande = random.randint(1, 20000)
    id_produit = random.randint(1, 500)
    quantite = random.randint(1, 5)
    prix_unitaire = df_produits.loc[df_produits['id_produit'] == id_produit, 'prix_unitaire'].values[0]

    lignes_data.append({
        'id_ligne': i,
        'id_commande': id_commande,
        'id_produit': id_produit,
        'quantite': quantite,
        'prix_unitaire': prix_unitaire
    })

    # Calcul du montant total de la commande
    montant_ligne = quantite * prix_unitaire
    if id_commande in montants_par_commande:
        montants_par_commande[id_commande] += montant_ligne
    else:
        montants_par_commande[id_commande] = montant_ligne

df_lignes = pd.DataFrame(lignes_data)
df_lignes.to_csv('lignes_commandes.csv', index=False, encoding='utf-8')
print(f"Fichier lignes_commandes.csv créé : {len(df_lignes)} lignes")

# Mise à jour des montants totaux dans commandes
df_commandes['montant_total'] = df_commandes['id_commande'].map(
    lambda x: round(montants_par_commande.get(x, 0), 2)
)
df_commandes.to_csv('commandes.csv', index=False, encoding='utf-8')
print(f"Fichier commandes.csv créé : {len(df_commandes)} lignes")

print("\nGénération terminée avec succès !")
print("Fichiers créés : clients.csv, produits.csv, commandes.csv, lignes_commandes.csv")