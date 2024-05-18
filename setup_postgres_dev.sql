-- create database --
CREATE DATABASE "fintech";
-- create role --
create role "fintech" with password 'pass123';
-- Grant login permissions --
alter role "fintech" with login;


-- grants roles -----
\c "fintech";
GRANT USAGE ON SCHEMA public TO "fintech";
GRANT CREATE ON SCHEMA public TO "fintech";
GRANT CONNECT ON DATABASE "fintech" TO "fintech";
GRANT ALL PRIVILEGES ON DATABASE "fintech" TO "fintech";


-- create tables 

CREATE TABLE cat_perfil_riesgo (
    cod_perfil_riesgo VARCHAR(50) PRIMARY KEY,
    perfil_riesgo VARCHAR(50) NOT NULL
);


CREATE TABLE catalogo_banca (
    cod_banca VARCHAR(50) PRIMARY KEY,
    banca VARCHAR(50) NOT NULL
);

CREATE TABLE catalogo_activos (
    cod_activo VARCHAR(50) PRIMARY KEY,
    activo VARCHAR(50) NOT NULL
);


CREATE TABLE historico_aba_macroactivos (
    id SERIAL PRIMARY KEY,
    ingestion_year INTEGER,
    ingestion_month INTEGER,
    ingestion_day INTEGER,                                                     
    id_sistema_cliente BIGINT,   
    macroactivo VARCHAR(50),
    cod_activo VARCHAR(50) REFERENCES catalogo_activos(cod_activo),
    aba INTEGER,
    cod_perfil_riesgo VARCHAR(50) REFERENCES cat_perfil_riesgo(cod_perfil_riesgo),
    cod_banca VARCHAR(50) REFERENCES catalogo_banca(cod_banca),
    year INTEGER,
    month INTEGER
);


-- Grant CRUD permissions --
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE ' || quote_ident(r.tablename) || ' TO fintech';
    END LOOP;
END $$;



-- Alternatively, grant specific permissions --
GRANT ALL ON SEQUENCE historico_aba_macroactivos_id_seq TO fintech;
-- Alternativamente, otorga permisos específicos
GRANT SELECT, UPDATE, USAGE ON SEQUENCE historico_aba_macroactivos_id_seq TO fintech;

-- Check permissions --
\dp historico_aba_macroactivos_id_seq

