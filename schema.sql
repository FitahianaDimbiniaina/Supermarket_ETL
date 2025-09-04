--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: deleted_non_numeric_codes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.deleted_non_numeric_codes (
    code text,
    libelle text,
    categorie text,
    fournisseur text
);


ALTER TABLE public.deleted_non_numeric_codes OWNER TO postgres;

--
-- Name: dim_client; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_client (
    client_id text,
    ville text,
    age integer,
    sexe text,
    fidelite text
);


ALTER TABLE public.dim_client OWNER TO postgres;

--
-- Name: dim_magasin; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_magasin (
    magasin_id bigint,
    ville text,
    state text,
    "Region" text,
    zip text
);


ALTER TABLE public.dim_magasin OWNER TO postgres;

--
-- Name: dim_produit; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_produit (
    code text,
    libelle text,
    categorie text
);


ALTER TABLE public.dim_produit OWNER TO postgres;

--
-- Name: dim_produit_backup_20240214; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_produit_backup_20240214 (
    code text,
    libelle text,
    categorie text,
    fournisseur text
);


ALTER TABLE public.dim_produit_backup_20240214 OWNER TO postgres;

--
-- Name: dim_temps; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_temps (
    date_id bigint,
    date timestamp without time zone,
    jour integer,
    semaine bigint,
    mois integer,
    annee integer,
    jour_semaine text,
    is_weekend boolean,
    is_ferie boolean
);


ALTER TABLE public.dim_temps OWNER TO postgres;

--
-- Name: fact_ventes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_ventes (
    vente_id integer NOT NULL,
    date_id integer NOT NULL,
    produit_id character varying NOT NULL,
    client_id character varying NOT NULL,
    magasin_id integer NOT NULL,
    montant numeric(12,2),
    quantite integer
);


ALTER TABLE public.fact_ventes OWNER TO postgres;

--
-- Name: fact_ventes_vente_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_ventes_vente_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_ventes_vente_id_seq OWNER TO postgres;

--
-- Name: fact_ventes_vente_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.fact_ventes_vente_id_seq OWNED BY public.fact_ventes.vente_id;


--
-- Name: staging_ventes_raw; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.staging_ventes_raw (
    "Row ID" bigint,
    "Order ID" text,
    "Order Date" text,
    "Ship Date" text,
    "Ship Mode" text,
    "Customer ID" text,
    "Segment" text,
    "Country" text,
    "City" text,
    "State" text,
    "Region" text,
    "Product ID" text,
    "Category" text,
    "Sub-Category" text,
    "Product Name" text,
    "Sales" double precision,
    "Quantity" bigint,
    "Discount" double precision,
    "Profit" double precision
);


ALTER TABLE public.staging_ventes_raw OWNER TO postgres;

--
-- Name: fact_ventes vente_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes ALTER COLUMN vente_id SET DEFAULT nextval('public.fact_ventes_vente_id_seq'::regclass);


--
-- Name: fact_ventes fact_ventes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes
    ADD CONSTRAINT fact_ventes_pkey PRIMARY KEY (vente_id);


--
-- Name: dim_client unique_client_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_client
    ADD CONSTRAINT unique_client_id UNIQUE (client_id);


--
-- Name: dim_produit unique_code; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_produit
    ADD CONSTRAINT unique_code UNIQUE (code);


--
-- Name: dim_temps unique_date_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_temps
    ADD CONSTRAINT unique_date_id UNIQUE (date_id);


--
-- Name: fact_ventes unique_fact_ventes; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes
    ADD CONSTRAINT unique_fact_ventes UNIQUE (date_id, produit_id, client_id, magasin_id);


--
-- Name: dim_magasin unique_magasin_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_magasin
    ADD CONSTRAINT unique_magasin_id UNIQUE (magasin_id);


--
-- Name: fact_ventes fk_client; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes
    ADD CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES public.dim_client(client_id);


--
-- Name: fact_ventes fk_date; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes
    ADD CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES public.dim_temps(date_id);


--
-- Name: fact_ventes fk_magasin; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes
    ADD CONSTRAINT fk_magasin FOREIGN KEY (magasin_id) REFERENCES public.dim_magasin(magasin_id);


--
-- Name: fact_ventes fk_produit; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_ventes
    ADD CONSTRAINT fk_produit FOREIGN KEY (produit_id) REFERENCES public.dim_produit(code);


--
-- PostgreSQL database dump complete
--

