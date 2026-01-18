-- ================================================================
-- Script : Création des tables sources OLTP
-- Base : retailpro_dwh
-- Description : Simule un système transactionnel
-- ================================================================

-- Table des clients
CREATE TABLE clients_source (
    client_id SERIAL PRIMARY KEY,        -- ID auto-incrémenté
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    email VARCHAR(200) UNIQUE NOT NULL,
    ville VARCHAR(100),
    segment VARCHAR(50),                 -- Bronze, Silver, Gold, Platinum
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des produits
CREATE TABLE produits_source (
    produit_id SERIAL PRIMARY KEY,
    nom_produit VARCHAR(200) NOT NULL,
    categorie VARCHAR(100),
    prix_unitaire DECIMAL(10,2) NOT NULL,
    cout_achat DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des ventes
CREATE TABLE ventes_source (
    vente_id SERIAL PRIMARY KEY,
    client_id INTEGER REFERENCES clients_source(client_id),
    produit_id INTEGER REFERENCES produits_source(produit_id),
    date_vente TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    quantite INTEGER NOT NULL,
    montant_total DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insérer des données de test
INSERT INTO clients_source (nom, prenom, email, ville, segment) VALUES
('Dupont', 'Jean', 'jean.dupont@email.com', 'Paris', 'Gold'),
('Martin', 'Marie', 'marie.martin@email.com', 'Lyon', 'Silver'),
('Bernard', 'Pierre', 'pierre.bernard@email.com', 'Marseille', 'Bronze');

INSERT INTO produits_source (nom_produit, categorie, prix_unitaire, cout_achat) VALUES
('Laptop HP', 'Informatique', 899.99, 650.00),
('Souris Logitech', 'Informatique', 29.99, 15.00),
('Clavier Mécanique', 'Informatique', 149.99, 80.00);

INSERT INTO ventes_source (client_id, produit_id, quantite, montant_total) VALUES
(1, 1, 1, 899.99),
(2, 2, 2, 59.98),
(3, 3, 1, 149.99);

-- Vérification
SELECT 'Clients:' as table_name, COUNT(*) as nb_lignes FROM clients_source
UNION ALL
SELECT 'Produits:', COUNT(*) FROM produits_source
UNION ALL
SELECT 'Ventes:', COUNT(*) FROM ventes_source;