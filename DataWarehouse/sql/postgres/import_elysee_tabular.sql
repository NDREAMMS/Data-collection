-- Import : données tabulaires Élysée + scores (mock) → PostgreSQL
-- \copy lit un fichier local ; le chemin ../../data/... part du répertoire courant.
-- Lancer psql depuis ce dossier (sql/postgres), par ex. :
--   cd sql/postgres
--   psql -U postgres -d immovision -f import_elysee_tabular.sql

\set ON_ERROR_STOP on

BEGIN;

DROP TABLE IF EXISTS elysee_tabular;

CREATE TABLE elysee_tabular (
    id BIGINT NOT NULL PRIMARY KEY,
    calculated_host_listings_count INTEGER,
    availability_365 SMALLINT,
    host_response_rate_num NUMERIC(6, 2),
    room_type_code SMALLINT,
    host_response_time_code SMALLINT,
    standardization_score SMALLINT NOT NULL,
    neighborhood_impact_score SMALLINT NOT NULL,
    CONSTRAINT chk_standardization_score CHECK (standardization_score IN (-1, 0, 1)),
    CONSTRAINT chk_neighborhood_impact_score CHECK (neighborhood_impact_score IN (-1, 0, 1))
);

\COPY elysee_tabular FROM 'C:/Users/Ndreamms/ImmoVision360_DataLake/DataWarehouse/postgres/elysee_tabular.csv' CSV HEADER;

COMMIT;
