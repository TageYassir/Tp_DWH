-- ========================================
-- CRÉATION DE LA BASE DE DONNÉES OLTP
-- ========================================

DROP DATABASE IF EXISTS ventes_oltp;
CREATE DATABASE ventes_oltp CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ventes_oltp;

-- ========================================
-- TABLE : clients
-- ========================================
CREATE TABLE clients (
    id_client INT AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    ville VARCHAR(100) NOT NULL,
    date_inscription DATE NOT NULL,
    INDEX idx_ville (ville),
    INDEX idx_date_inscription (date_inscription)
) ENGINE=InnoDB;

-- ========================================
-- TABLE : produits
-- ========================================
CREATE TABLE produits (
    id_produit INT AUTO_INCREMENT PRIMARY KEY,
    nom_produit VARCHAR(200) NOT NULL,
    categorie VARCHAR(100) NOT NULL,
    prix_unitaire DECIMAL(10,2) NOT NULL,
    INDEX idx_categorie (categorie)
) ENGINE=InnoDB;

-- ========================================
-- TABLE : commandes
-- ========================================
CREATE TABLE commandes (
    id_commande INT AUTO_INCREMENT PRIMARY KEY,
    id_client INT NOT NULL,
    date_commande DATE NOT NULL,
    montant_total DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (id_client) REFERENCES clients(id_client) ON DELETE CASCADE,
    INDEX idx_client (id_client),
    INDEX idx_date (date_commande)
) ENGINE=InnoDB;

-- ========================================
-- TABLE : lignes_commandes
-- ========================================
CREATE TABLE lignes_commandes (
    id_ligne INT AUTO_INCREMENT PRIMARY KEY,
    id_commande INT NOT NULL,
    id_produit INT NOT NULL,
    quantite INT NOT NULL,
    prix_unitaire DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (id_commande) REFERENCES commandes(id_commande) ON DELETE CASCADE,
    FOREIGN KEY (id_produit) REFERENCES produits(id_produit) ON DELETE CASCADE,
    INDEX idx_commande (id_commande),
    INDEX idx_produit (id_produit)
) ENGINE=InnoDB;

-- ========================================
-- VÉRIFICATION DE LA STRUCTURE
-- ========================================
SHOW TABLES;
DESCRIBE clients;
DESCRIBE produits;
DESCRIBE commandes;
DESCRIBE lignes_commandes;