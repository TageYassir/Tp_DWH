-- ================================================================
-- HUB CLIENT
-- ================================================================
CREATE TABLE hub_client (
    -- Hash MD5 de la clé naturelle (client_id)
    client_hash_key VARCHAR(32) PRIMARY KEY,
    
    -- Clé naturelle (business key)
    client_id INTEGER NOT NULL UNIQUE,
    
    -- Métadonnées Data Vault
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) DEFAULT 'OLTP'
);

-- EXPLICATION :
-- client_hash_key : MD5(client_id) pour anonymiser et standardiser
-- client_id : la vraie clé métier
-- load_date : quand cette entité est apparue la première fois
-- record_source : d'où vient cette donnée

-- ================================================================
-- HUB PRODUIT
-- ================================================================
CREATE TABLE hub_produit (
    produit_hash_key VARCHAR(32) PRIMARY KEY,
    produit_id INTEGER NOT NULL UNIQUE,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) DEFAULT 'OLTP'
);

-- ================================================================
-- LINK VENTE (relation Client-Produit)
-- ================================================================
CREATE TABLE link_vente (
    -- Hash MD5 composé de vente_id + client_id + produit_id
    vente_hash_key VARCHAR(32) PRIMARY KEY,
    
    -- Références aux hubs
    client_hash_key VARCHAR(32) NOT NULL,
    produit_hash_key VARCHAR(32) NOT NULL,
    
    -- Clé naturelle de la transaction
    vente_id INTEGER NOT NULL,
    
    -- Métadonnées
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(50) DEFAULT 'OLTP',
    
    -- Contraintes
    FOREIGN KEY (client_hash_key) REFERENCES hub_client(client_hash_key),
    FOREIGN KEY (produit_hash_key) REFERENCES hub_produit(produit_hash_key)
);

-- ================================================================
-- SATELLITE CLIENT
-- ================================================================
CREATE TABLE sat_client (
    -- Clés composites (PK)
    client_hash_key VARCHAR(32) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (client_hash_key, load_date),
    
    -- Attributs descriptifs
    nom VARCHAR(100),
    prenom VARCHAR(100),
    email VARCHAR(200),
    ville VARCHAR(100),
    segment VARCHAR(50),
    
    -- Métadonnées
    load_end_date TIMESTAMP,  -- NULL = version actuelle
    record_source VARCHAR(50),
    hash_diff VARCHAR(32),    -- Hash de tous les attributs
    
    -- Contrainte
    FOREIGN KEY (client_hash_key) REFERENCES hub_client(client_hash_key)
);

-- EXPLICATION :
-- Clé primaire composite : (client_hash_key, load_date)
-- Permet plusieurs versions du même client à différentes dates
-- hash_diff : pour détecter rapidement si les données ont changé

-- ================================================================
-- SATELLITE PRODUIT
-- ================================================================
CREATE TABLE sat_produit (
    produit_hash_key VARCHAR(32) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (produit_hash_key, load_date),
    
    nom_produit VARCHAR(200),
    categorie VARCHAR(100),
    prix_unitaire DECIMAL(10,2),
    cout_achat DECIMAL(10,2),
    
    load_end_date TIMESTAMP,
    record_source VARCHAR(50),
    hash_diff VARCHAR(32),
    
    FOREIGN KEY (produit_hash_key) REFERENCES hub_produit(produit_hash_key)
);

-- ================================================================
-- SATELLITE VENTE
-- ================================================================
CREATE TABLE sat_vente (
    vente_hash_key VARCHAR(32) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (vente_hash_key, load_date),
    
    date_vente TIMESTAMP,
    quantite INTEGER,
    montant_total DECIMAL(10,2),
    
    load_end_date TIMESTAMP,
    record_source VARCHAR(50),
    hash_diff VARCHAR(32),
    
    FOREIGN KEY (vente_hash_key) REFERENCES link_vente(vente_hash_key)
);

SELECT 'Tables Data Vault créées avec succès' as resultat;