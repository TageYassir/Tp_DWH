# ================================================================
# Script : Chargement SCD Type 2 pour dim_client
# Description : Charge et historise les clients depuis la source
# ================================================================

# ================================================================
# PARTIE 1 : IMPORTS
# ================================================================
import psycopg2  # Bibliothèque pour se connecter à PostgreSQL
from datetime import datetime, date

# EXPLICATION :
# - psycopg2 est un driver Python NATIF pour PostgreSQL
# - Différent de JDBC (qui est pour Java/Spark)
# - Ici on utilise Python pur, pas PySpark

print("Bibliothèques importées")

# ================================================================
# PARTIE 2 : CONFIGURATION DE LA CONNEXION
# ================================================================

# Paramètres de connexion à PostgreSQL
DB_CONFIG = {
    'host': 'localhost',  # Serveur PostgreSQL (votre machine)
    'port': 5432,  # Port par défaut de PostgreSQL
    'database': 'retailpro_dwh',  # Nom de la base de données
    'user': 'postgres',  # Utilisateur PostgreSQL
    'password': '1234'  # IMPORTANT : Mettez votre mot de passe ici
}

# EXPLICATION :
# Ce dictionnaire contient toutes les informations nécessaires
# pour que Python se connecte à PostgreSQL

print(f"Configuration : connexion à {DB_CONFIG['database']} sur {DB_CONFIG['host']}")


# ================================================================
# PARTIE 3 : FONCTION DE CONNEXION
# ================================================================

def creer_connexion():
    """
    Crée une connexion à PostgreSQL
    Returns: objet connexion ou None si erreur
    """
    try:
        # Tentative de connexion avec les paramètres
        conn = psycopg2.connect(**DB_CONFIG)
        # Le ** "déplie" le dictionnaire comme arguments
        # Équivalent à : psycopg2.connect(host='localhost', port=5432, ...)

        print("✓ Connexion à PostgreSQL réussie")
        return conn
    except Exception as e:
        # Si erreur (mot de passe incorrect, PostgreSQL arrêté, etc.)
        print(f"✗ Erreur de connexion : {e}")
        return None


# ================================================================
# PARTIE 4 : FONCTION DE LECTURE DES CLIENTS SOURCE
# ================================================================

def lire_clients_source(conn):
    """
    Lit tous les clients depuis la table source
    Args: conn = connexion PostgreSQL
    Returns: liste de tuples (client_id, nom, prenom, email, ville, segment)
    """
    # Créer un curseur pour exécuter des requêtes
    cur = conn.cursor()
    # EXPLICATION :
    # Un curseur est comme un pointeur qui permet d'exécuter des requêtes SQL

    # Requête SQL pour lire les clients
    sql = """
        SELECT client_id, nom, prenom, email, ville, segment
        FROM clients_source
        ORDER BY client_id
    """

    # Exécuter la requête
    cur.execute(sql)
    # EXPLICATION :
    # execute() envoie la requête SQL à PostgreSQL

    # Récupérer tous les résultats
    clients = cur.fetchall()
    # EXPLICATION :
    # fetchall() récupère TOUTES les lignes retournées par la requête
    # Retourne une liste de tuples : [(1, 'Dupont', 'Jean', ...), (2, 'Martin', 'Marie', ...)]

    cur.close()

    print(f"✓ {len(clients)} clients lus depuis la source")
    return clients


# ================================================================
# PARTIE 5 : VÉRIFIER SI UN CLIENT EXISTE DÉJÀ
# ================================================================

def client_existe_dans_dimension(conn, client_id):
    """
    Vérifie si un client existe déjà dans la dimension
    Args:
        conn = connexion PostgreSQL
        client_id = ID du client à chercher
    Returns:
        tuple (données client) si trouvé
        None si pas trouvé
    """
    cur = conn.cursor()

    # Chercher la version COURANTE du client
    sql = """
        SELECT client_key, client_id, nom, prenom, email, ville, segment, version
        FROM dim_client
        WHERE client_id = %s 
          AND est_courant = TRUE
    """
    # EXPLICATION :
    # - %s est un placeholder (emplacement) pour éviter les injections SQL
    # - WHERE client_id = %s : cherche ce client spécifique
    # - AND est_courant = TRUE : seulement la version actuelle

    cur.execute(sql, (client_id,))
    # EXPLICATION :
    # - Le tuple (client_id,) remplace le %s dans la requête
    # - La virgule après client_id est IMPORTANTE pour créer un tuple

    resultat = cur.fetchone()
    # EXPLICATION :
    # fetchone() récupère UNE SEULE ligne (la première)
    # Retourne None s'il n'y a pas de résultat

    cur.close()
    return resultat


# ================================================================
# PARTIE 6 : DÉTECTER LES CHANGEMENTS
# ================================================================

def detecter_changement(client_source, client_dimension):
    """
    Compare les données source et dimension pour détecter des changements
    Args:
        client_source = tuple depuis clients_source
        client_dimension = tuple depuis dim_client
    Returns:
        True si changement détecté, False sinon
    """
    # Extraire les valeurs à comparer
    # client_source : (client_id, nom, prenom, email, ville, segment)
    # Indices :         0          1    2       3      4      5

    source_email = client_source[3]
    source_ville = client_source[4]
    source_segment = client_source[5]

    # client_dimension : (client_key, client_id, nom, prenom, email, ville, segment, version)
    # Indices :           0           1          2    3       4      5      6        7

    dim_email = client_dimension[4]
    dim_ville = client_dimension[5]
    dim_segment = client_dimension[6]

    # Comparer les attributs
    if source_email != dim_email:
        print(f"  Changement détecté : email {dim_email} → {source_email}")
        return True

    if source_ville != dim_ville:
        print(f"  Changement détecté : ville {dim_ville} → {source_ville}")
        return True

    if source_segment != dim_segment:
        print(f"  Changement détecté : segment {dim_segment} → {source_segment}")
        return True

    # Aucun changement
    return False


# ================================================================
# PARTIE 7 : INSÉRER UN NOUVEAU CLIENT (Version 1)
# ================================================================

def inserer_nouveau_client(conn, client_data):
    """
    Insère un nouveau client dans la dimension (version 1)
    Args:
        conn = connexion PostgreSQL
        client_data = tuple (client_id, nom, prenom, email, ville, segment)
    """
    cur = conn.cursor()

    sql = """
        INSERT INTO dim_client 
        (client_id, nom, prenom, email, ville, segment, 
         date_debut, date_fin, est_courant, version)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, NULL, TRUE, 1)
        RETURNING client_key
    """
    # EXPLICATION :
    # - INSERT INTO dim_client : insérer dans la table
    # - VALUES (%s, %s, ...) : 6 placeholders pour les 6 valeurs
    # - CURRENT_DATE : fonction PostgreSQL qui donne la date d'aujourd'hui
    # - NULL : date_fin est NULL car c'est la version courante
    # - TRUE : est_courant = TRUE car c'est la version actuelle
    # - 1 : version = 1 car c'est la première version
    # - RETURNING client_key : PostgreSQL retourne l'ID auto-généré

    cur.execute(sql, client_data)
    # client_data contient les 6 valeurs qui remplacent les %s

    client_key = cur.fetchone()[0]
    # EXPLICATION :
    # fetchone() récupère le client_key retourné par RETURNING
    # [0] prend le premier élément du tuple

    conn.commit()
    # EXPLICATION :
    # commit() valide la transaction dans PostgreSQL
    # Sans commit(), les changements sont perdus !

    cur.close()
    print(f"✓ Nouveau client inséré : client_id={client_data[0]}, client_key={client_key}")
    return client_key


# ================================================================
# PARTIE 8 : FERMER LA VERSION COURANTE
# ================================================================

def fermer_version_courante(conn, client_key):
    """
    Ferme la version courante d'un client
    Args:
        conn = connexion
        client_key = clé de la version à fermer
    """
    cur = conn.cursor()

    sql = """
        UPDATE dim_client
        SET date_fin = CURRENT_DATE - INTERVAL '1 day',
            est_courant = FALSE
        WHERE client_key = %s
    """
    # EXPLICATION :
    # - UPDATE dim_client : modifier la table
    # - SET date_fin = CURRENT_DATE - INTERVAL '1 day' :
    #   mettre date_fin à hier (la nouvelle version commence aujourd'hui)
    # - est_courant = FALSE : cette version n'est plus courante
    # - WHERE client_key = %s : seulement pour cette version spécifique

    cur.execute(sql, (client_key,))
    conn.commit()
    cur.close()

    print(f"✓ Version fermée : client_key={client_key}")


# ================================================================
# PARTIE 9 : CRÉER UNE NOUVELLE VERSION
# ================================================================

def creer_nouvelle_version(conn, client_data, ancienne_version):
    """
    Crée une nouvelle version d'un client existant
    Args:
        conn = connexion
        client_data = nouvelles données
        ancienne_version = numéro de l'ancienne version
    """
    cur = conn.cursor()

    nouvelle_version = ancienne_version + 1

    sql = """
        INSERT INTO dim_client 
        (client_id, nom, prenom, email, ville, segment, 
         date_debut, date_fin, est_courant, version)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, NULL, TRUE, %s)
        RETURNING client_key
    """
    # EXPLICATION :
    # Même chose que l'insertion, mais avec version = ancienne_version + 1

    # Créer le tuple avec la nouvelle version
    data_avec_version = client_data + (nouvelle_version,)
    # EXPLICATION :
    # Si client_data = (1, 'Dupont', 'Jean', ...)
    # Alors client_data + (2,) = (1, 'Dupont', 'Jean', ..., 2)

    cur.execute(sql, data_avec_version)
    client_key = cur.fetchone()[0]
    conn.commit()
    cur.close()

    print(f"✓ Nouvelle version créée : client_id={client_data[0]}, version={nouvelle_version}, client_key={client_key}")
    return client_key


# ================================================================
# PARTIE 10 : FONCTION PRINCIPALE DE TRAITEMENT
# ================================================================

def traiter_scd_type2():
    """
    Fonction principale qui orchestre tout le processus SCD Type 2
    """
    print("\n" + "=" * 70)
    print("DÉBUT DU PROCESSUS SCD TYPE 2")
    print("=" * 70 + "\n")

    # Étape 1 : Se connecter à PostgreSQL
    conn = creer_connexion()
    if not conn:
        print("Impossible de continuer sans connexion")
        return

    # Étape 2 : Lire les clients depuis la source
    clients_source = lire_clients_source(conn)

    # Compteurs pour statistiques
    nb_nouveaux = 0
    nb_nouvelles_versions = 0
    nb_inchanges = 0

    # Étape 3 : Traiter chaque client
    for client in clients_source:
        client_id = client[0]
        print(f"\nTraitement du client {client_id}...")

        # Vérifier si le client existe déjà
        client_dim = client_existe_dans_dimension(conn, client_id)

        if client_dim is None:
            # CAS 1 : Nouveau client
            print(f"→ Nouveau client")
            inserer_nouveau_client(conn, client)
            nb_nouveaux += 1

        else:
            # CAS 2 : Client existant - vérifier changements
            if detecter_changement(client, client_dim):
                print(f"→ Changement détecté")
                # Fermer l'ancienne version
                fermer_version_courante(conn, client_dim[0])
                # Créer la nouvelle version
                creer_nouvelle_version(conn, client, client_dim[7])
                nb_nouvelles_versions += 1
            else:
                print(f"→ Aucun changement")
                nb_inchanges += 1

    # Étape 4 : Afficher les statistiques
    print("\n" + "=" * 70)
    print("RÉSUMÉ DU CHARGEMENT")
    print("=" * 70)
    print(f"Nouveaux clients insérés : {nb_nouveaux}")
    print(f"Nouvelles versions créées : {nb_nouvelles_versions}")
    print(f"Clients inchangés : {nb_inchanges}")
    print(f"Total traité : {len(clients_source)}")
    print("=" * 70)

    # Fermer la connexion
    conn.close()
    print("\nConnexion fermée")


# ================================================================
# PARTIE 11 : POINT D'ENTRÉE DU SCRIPT
# ================================================================

if __name__ == "__main__":
    # EXPLICATION :
    # Cette ligne vérifie si le script est exécuté directement
    # (et non importé comme module)

    traiter_scd_type2()

    print("\n✓ Script terminé avec succès")