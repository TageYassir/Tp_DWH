-- ========================================
-- CRÉATION DE LA BASE DE DONNÉES DWH
-- ========================================

DROP DATABASE IF EXISTS ventes_dwh;
CREATE DATABASE ventes_dwh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ventes_dwh;

-- ========================================
-- DIMENSION : DimClient
-- ========================================
CREATE TABLE DimClient (
    id_client_dim INT AUTO_INCREMENT PRIMARY KEY,
    id_client_source INT NOT NULL,
    nom_complet VARCHAR(200) NOT NULL,
    email VARCHAR(150) NOT NULL,
    ville VARCHAR(100) NOT NULL,
    UNIQUE KEY uk_client_source (id_client_source),
    INDEX idx_ville (ville)
) ENGINE=InnoDB;

-- ========================================
-- DIMENSION : DimProduit
-- ========================================
CREATE TABLE DimProduit (
    id_produit_dim INT AUTO_INCREMENT PRIMARY KEY,
    id_produit_source INT NOT NULL,
    nom_produit VARCHAR(200) NOT NULL,
    categorie VARCHAR(100) NOT NULL,
    UNIQUE KEY uk_produit_source (id_produit_source),
    INDEX idx_categorie (categorie)
) ENGINE=InnoDB;

-- ========================================
-- DIMENSION : DimDate
-- ========================================
CREATE TABLE DimDate (
    id_date_dim INT PRIMARY KEY,
    date_complete DATE NOT NULL UNIQUE,
    annee INT NOT NULL,
    trimestre INT NOT NULL,
    mois INT NOT NULL,
    nom_mois VARCHAR(20) NOT NULL,
    jour INT NOT NULL,
    jour_semaine INT NOT NULL,
    nom_jour VARCHAR(20) NOT NULL,
    INDEX idx_annee (annee),
    INDEX idx_mois (mois),
    INDEX idx_trimestre (trimestre)
) ENGINE=InnoDB;

-- ========================================
-- TABLE DE FAITS : FactVentes
-- ========================================
CREATE TABLE FactVentes (
    id_vente INT AUTO_INCREMENT PRIMARY KEY,
    id_client_dim INT NOT NULL,
    id_produit_dim INT NOT NULL,
    id_date_dim INT NOT NULL,
    quantite INT NOT NULL,
    prix_unitaire DECIMAL(10,2) NOT NULL,
    montant_total DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (id_client_dim) REFERENCES DimClient(id_client_dim),
    FOREIGN KEY (id_produit_dim) REFERENCES DimProduit(id_produit_dim),
    FOREIGN KEY (id_date_dim) REFERENCES DimDate(id_date_dim),
    INDEX idx_client (id_client_dim),
    INDEX idx_produit (id_produit_dim),
    INDEX idx_date (id_date_dim)
) ENGINE=InnoDB;

-- ========================================
-- VÉRIFICATION DE LA STRUCTURE
-- ========================================
SHOW TABLES;
DESCRIBE DimClient;
DESCRIBE DimProduit;
DESCRIBE DimDate;
DESCRIBE FactVentes;