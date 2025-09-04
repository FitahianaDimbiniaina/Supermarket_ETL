from scripts.analysis.fact_vente_mart import (
    top_10_products_by_quantity,
    top_sales_by_region,
    top_categories_june2017,
    top_products_east_jan2017,
    top_categories_june2017    
)
from scripts.analysis.fact_fidelite_mart import (
    most_loyal_to_Hicks_white,
    top_10_most_magasin_with_loyal_customer,
    top_loyal_west_h2_2017,
    loyalty_march2017,
)

def main():
    top_sales_by_region.run()
    top_10_products_by_quantity.run()
    top_categories_june2017.run()
    top_products_east_jan2017.run()
    top_categories_june2017.run()

    most_loyal_to_Hicks_white.run()
    top_10_most_magasin_with_loyal_customer.run()
    loyalty_march2017.run()
    top_loyal_west_h2_2017.run()

if __name__ == '__main__':
    main()
