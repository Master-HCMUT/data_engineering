# Ecommerce Data Engineering

This project builds an e-commerce analytics pipeline on top of the AdventureWorks OLTP CSV dataset using a medallion architecture:
- `bronze`: raw source ingestion
- `silver`: cleaned and conformed business entities
- `gold`: star schema and business-facing marts

## Gold Model

The current gold layer is organized around a sales order line fact table and supporting dimensions:
- `dev.gold.fact_sales_order_line`
- `dev.gold.dim_date`
- `dev.gold.dim_product`
- `dev.gold.dim_customer`
- `dev.gold.dim_sales_territory`
- `dev.gold.dim_salesperson`

From that star schema, the project exposes business marts for reporting and analysis:
- `dev.gold.mart_monthly_revenue`
- `dev.gold.mart_product_popularity`
- `dev.gold.mart_monthly_territory_sales`
- `dev.gold.mart_salesperson_performance`
- `dev.gold.mart_customer_value`

## Business Requirements

The gold layer currently covers five business requirements for the sales analytics scope.

### BR-01: Monthly Revenue Trend

Business users need to monitor revenue over time to understand sales seasonality and overall commercial performance.

Output mart:
- `dev.gold.mart_monthly_revenue`

Main metrics:
- monthly net revenue
- monthly gross revenue
- discount amount
- order count
- total quantity sold

### BR-02: Popular Products

Business users need to identify the most popular products by quantity and by revenue to support merchandising, promotion planning, and inventory prioritization.

Output mart:
- `dev.gold.mart_product_popularity`

Main metrics:
- total quantity sold per product
- net sales amount per product
- gross sales amount per product
- popularity rank by quantity
- popularity rank by revenue

### BR-03: Monthly Territory Performance

Business users need to compare sales performance across territories by month to identify high-performing markets and weak regions that may need intervention.

Output mart:
- `dev.gold.mart_monthly_territory_sales`

Main metrics:
- monthly order count by territory
- monthly customer count by territory
- monthly quantity sold by territory
- monthly gross and net revenue by territory
- average order value by territory

### BR-04: Salesperson Performance

Business users need to evaluate salesperson contribution to revenue, order volume, and customer coverage in order to track individual performance and support sales management decisions.

Output mart:
- `dev.gold.mart_salesperson_performance`

Main metrics:
- order count by salesperson
- customer count by salesperson
- total quantity sold by salesperson
- gross and net revenue by salesperson
- average order value by salesperson
- revenue rank by salesperson

### BR-05: Customer Value Analysis

Business users need to identify high-value customers and compare purchasing behavior between individual customers and store customers.

Output mart:
- `dev.gold.mart_customer_value`

Main metrics:
- order count by customer
- total quantity sold by customer
- gross and net revenue by customer
- average order value by customer
- first and last order dates
- customer value rank
