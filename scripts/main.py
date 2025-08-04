from scripts.analysis import (
    top_10_sales_by_region,
    top_10_products_by_quantity,
    top_10_products_by_region_limited,
    correlation_category_recurrence,
    top_10_sales_per_years  # ⬅️ add this
)

def main():
    top_10_sales_by_region.run()
    top_10_products_by_quantity.run()
    top_10_sales_per_years.run()  # ⬅️ and this
    # top_10_products_by_region_limited.run()
    # correlation_category_recurrence.run()

if __name__ == '__main__':
    main()
