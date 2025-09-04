-- Top 3 products by sales in the Eastern region for January 2017
CREATE OR REPLACE VIEW top_products_east_jan2017 AS
SELECT
    dp.libelle AS product_name,
    SUM(fv.montant) AS total_sales
FROM fact_ventes fv
JOIN dim_produit dp ON fv.produit_id = dp.code
JOIN dim_magasin dm ON fv.magasin_id = dm.magasin_id
JOIN dim_temps dt ON fv.date_id = dt.date_id
WHERE dm."Region" = 'East'
  AND dt.date BETWEEN '2017-01-01' AND '2017-01-31'
GROUP BY dp.libelle
ORDER BY total_sales DESC
LIMIT 3;

-- Total loyalty points per client in March 2017
CREATE OR REPLACE VIEW loyalty_march2017 AS
SELECT
    f.client_id,
    SUM(f.points_earned) AS total_points
FROM fact_fidelite f
JOIN dim_temps dt ON f.date_id = dt.date_id
WHERE dt.date BETWEEN '2017-03-01' AND '2017-03-31'
GROUP BY f.client_id
ORDER BY total_points DESC
LIMIT 10;

-- Top 5 regions by sales in Q2 2017
CREATE OR REPLACE VIEW top_regions_q2_2017 AS
SELECT
    dm."Region",
    SUM(fv.montant) AS total_sales
FROM fact_ventes fv
JOIN dim_magasin dm ON fv.magasin_id = dm.magasin_id
JOIN dim_temps dt ON fv.date_id = dt.date_id
WHERE dt.date BETWEEN '2017-04-01' AND '2017-06-30'
GROUP BY dm."Region"
ORDER BY total_sales DESC
LIMIT 5;

-- Most sold product categories in June 2017
CREATE OR REPLACE VIEW top_categories_june2017 AS
SELECT
    dp.categorie,
    SUM(fv.montant) AS total_sales
FROM fact_ventes fv
JOIN dim_produit dp ON fv.produit_id = dp.code
JOIN dim_temps dt ON fv.date_id = dt.date_id
WHERE dt.date BETWEEN '2017-06-01' AND '2017-06-30'
GROUP BY dp.categorie
ORDER BY total_sales DESC;

-- Top loyal clients in the Western region for 2017 H2 (Jul-Dec)
CREATE OR REPLACE VIEW top_loyal_west_h2_2017 AS
SELECT
    f.client_id,
    SUM(f.points_earned) AS total_points
FROM fact_fidelite f
JOIN dim_magasin dm ON f.magasin_id = dm.magasin_id
JOIN dim_temps dt ON f.date_id = dt.date_id
WHERE dm."Region" = 'West'
  AND dt.date BETWEEN '2017-07-01' AND '2017-12-31'
GROUP BY f.client_id
ORDER BY total_points DESC
LIMIT 10;
