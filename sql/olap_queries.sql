-- top_10_products_by_region_limited
SELECT region, libelle AS product_name, total_sales
FROM (
    SELECT 
        dm.region,
        dp.libelle,
        SUM(fv.montant) AS total_sales,
        RANK() OVER (PARTITION BY dm.region ORDER BY SUM(fv.montant) DESC) AS rank
    FROM fact_ventes fv
    JOIN dim_produit dp ON fv.produit_id = dp.code
    JOIN dim_magasin dm ON fv.magasin_id = dm.magasin_id
    GROUP BY dm.region, dp.libelle
) ranked
WHERE rank <= 10
ORDER BY region, total_sales DESC;

-- correlation_category_recurrence
SELECT
    dc.client_id,
    dp.categorie,
    COUNT(DISTINCT dt.date) AS purchase_days
FROM fact_ventes fv
JOIN dim_client dc ON fv.client_id::int = dc.client_id
JOIN dim_temps dt ON fv.date_id = dt.date_id
JOIN dim_produit dp ON fv.produit_id = dp.code
GROUP BY dc.client_id, dp.categorie;

-- top_3_regions_by_sales
SELECT dm."Region", SUM(fv.montant) AS total_sales
FROM fact_ventes fv
JOIN dim_magasin dm ON fv.magasin_id = dm.magasin_id
GROUP BY dm."Region"
ORDER BY total_sales DESC
LIMIT 3;

-- top_10_products_by_quantity_in_region
SELECT dp.libelle AS product_name, SUM(fv.montant) AS total_sales
FROM fact_ventes fv
JOIN dim_produit dp ON fv.produit_id = dp.code
JOIN dim_magasin dm ON fv.magasin_id = dm.magasin_id
WHERE dm."Region" = :region
GROUP BY dp.libelle
ORDER BY total_sales DESC
LIMIT 10;

-- top_10_products_by_quantity
SELECT 
    dp.code,
    dp.libelle AS product_name, 
    SUM(fv.montant) AS total_sales
FROM fact_ventes fv
JOIN dim_produit dp ON fv.produit_id = dp.code
GROUP BY dp.code, dp.libelle
ORDER BY total_sales DESC
LIMIT 10;

-- top_10_products_by_year
SELECT annee, product_name, total_sales
FROM (
    SELECT 
        dt.annee,
        dp.libelle AS product_name,
        SUM(fv.montant) AS total_sales,
        RANK() OVER (PARTITION BY dt.annee ORDER BY SUM(fv.montant) DESC) AS rank
    FROM fact_ventes fv
    JOIN dim_produit dp ON fv.produit_id = dp.code
    JOIN dim_temps dt ON fv.date_id = dt.date_id
    GROUP BY dt.annee, dp.libelle
) ranked
WHERE rank <= 10
ORDER BY annee, total_sales DESC;
