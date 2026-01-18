-- ================================================================
-- Table : dim_client avec SCD Type 2
-- Description : Dimension client avec historisation complète
-- ================================================================

CREATE TABLE dim_client (
    -- Clé surrogate (technique) - unique pour chaque VERSION
    client_key SERIAL PRIMARY KEY,
    
    -- Clé naturelle (métier) - peut avoir plusieurs versions
    client_id INTEGER NOT NULL,
    
    -- Attributs descriptifs
    nom VARCHAR(100),
    prenom VARCHAR(100),
    email VARCHAR(200),
    ville VARCHAR(100),
    segment VARCHAR(50),
    
    -- Colonnes SCD Type 2
    date_debut DATE NOT NULL DEFAULT CURRENT_DATE,
    date_fin DATE,                    -- NULL = version actuelle
    est_courant BOOLEAN DEFAULT TRUE, -- TRUE = version actuelle
    version INTEGER DEFAULT 1,        -- Numéro de version
    
    -- Métadonnées
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT unique_client_version UNIQUE (client_id, version)
);

-- Index pour performance
CREATE INDEX idx_dim_client_id ON dim_client(client_id);
CREATE INDEX idx_dim_client_courant ON dim_client(est_courant) WHERE est_courant = TRUE;

SELECT 'Table dim_client créée avec succès' as resultat;