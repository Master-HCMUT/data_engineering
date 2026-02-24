# Business Requirements

In this section, we outline the business requirements for the data engineering project based on the **AdventureWorks** OLTP dataset — a fictitious bicycle e-commerce company that sells bikes, components, clothing, and accessories to online customers and resellers.

The scope of this project is focused on the **e-commerce domain** (online sales, product catalogue, and customer behaviour). There are **8 business requirements (BR)** in total, numbered BR-01 through BR-08.

---

## Overview

| BR ID | Area | Title |
|---|---|---|
| BR-01 | Data Ingestion | Ingest all OLTP CSV data into Databricks |
| BR-02 | Data Storage | Bronze layer — raw data archival |
| BR-03 | Data Storage | Silver layer — cleaned & enriched e-commerce data |
| BR-04 | Data Storage | Gold layer — e-commerce aggregates |
| BR-05 | Data Processing | Real-time product interaction feature vector |
| BR-06 | Data Processing | Batch trending product computation |
| BR-07 | Business Intelligence | E-commerce analytics dashboard |
| BR-08 | AI / ML | Feature store & product recommendation serving |

---

## BR-01 — Ingest All OLTP CSV Data into Databricks

- **Area:** Data Ingestion (E in ELT)
- **Description:** The system must ingest **all** CSV files from the AdventureWorks OLTP source into a Databricks Unity Catalog Volume. Although the downstream processing focuses on the e-commerce domain, the full dataset is ingested to preserve the complete historical archive and allow future expansion.
- **Data Path:** `sql-server-samples/samples/databases/adventure-works/oltp-install-script`
- **All tables to ingest (~70 CSV files):**

  | Domain | Tables |
  |---|---|
  | **Sales** | `SalesOrderHeader`, `SalesOrderDetail`, `SalesOrderHeaderSalesReason`, `SalesReason`, `ShoppingCartItem`, `SpecialOffer`, `SpecialOfferProduct`, `CreditCard`, `PersonCreditCard`, `CurrencyRate`, `Currency`, `CountryRegionCurrency`, `SalesTaxRate` |
  | **Product** | `Product`, `ProductCategory`, `ProductSubcategory`, `ProductModel`, `ProductDescription`, `ProductReview`, `ProductListPriceHistory`, `ProductCostHistory`, `ProductInventory`, `ProductPhoto`, `ProductVendor` |
  | **Customer & Person** | `Customer`, `Person`, `Store`, `EmailAddress`, `PersonPhone`, `PhoneNumberType`, `Address`, `StateProvince`, `CountryRegion`, `BusinessEntity`, `BusinessEntityAddress` |
  | **Sales Force** | `SalesPerson`, `SalesPersonQuotaHistory`, `SalesTerritory`, `SalesTerritoryHistory` |
  | **Purchasing** | `PurchaseOrderHeader`, `PurchaseOrderDetail`, `Vendor`, `ShipMethod` |
  | **Production** | `WorkOrder`, `WorkOrderRouting`, `BillOfMaterials`, `Location`, `TransactionHistory`, `ScrapReason` |
  | **HR** | `Employee`, `Department`, `EmployeeDepartmentHistory`, `EmployeePayHistory`, `Shift` |

- **Method:** Databricks CLI or Python SDK (`databricks.sdk`).
- **Target:** Databricks Unity Catalog Volume (e.g., `catalog.raw_volume.adventureworks/`).
- **Frequency:** One-time bulk load; pipeline must be idempotent (safe to re-run).

---

## BR-02 — Bronze Layer: Raw Data Archival

- **Area:** Data Storage — [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- **Description:** The system must load all ingested CSV files into Bronze Delta Lake tables as-is, without any schema transformation. The Bronze layer serves as the cold-storage, immutable archive of the source system, supporting data lineage, auditability, and reprocessing.
- **Requirements:**
  - One Delta table per CSV source file, stored under the `bronze` schema.
  - All original columns preserved with their raw string types.
  - Two metadata columns appended to every record:
    - `_ingested_at` (timestamp) — when the record was loaded.
    - `_source_file` (string) — path of the originating CSV file.
- **Format:** Delta Lake.

---

## BR-03 — Silver Layer: Cleaned & Enriched E-Commerce Data

- **Area:** Data Storage — Medallion Architecture
- **Description:** The system must transform the Bronze raw data into clean, typed, and enriched Silver Delta tables, **scoped to the e-commerce domain**. Silver tables are the source of truth for all downstream BI and ML workloads.
- **In-scope e-commerce tables:**
  - `silver_sales_order` — merged `SalesOrderHeader` + `SalesOrderDetail`, online channel only (`OnlineOrderFlag = 1`).
  - `silver_product` — `Product` enriched with `ProductSubcategory` and `ProductCategory` names.
  - `silver_customer` — `Customer` joined with `Person`, `Address`, `StateProvince`, `SalesTerritory`.
  - `silver_product_review` — cleaned `ProductReview` with sentiment-ready text fields.
  - `silver_shopping_cart` — `ShoppingCartItem` with resolved product details.
  - `silver_special_offer` — `SpecialOffer` joined with `SpecialOfferProduct`.
- **Transformations applied to all Silver tables:**
  - Data type casting (dates, decimals, integers).
  - Null handling and deduplication on primary keys.
  - Column renaming to snake_case business-friendly names.
  - Filtering out non-online or invalid records where applicable.

---

## BR-04 — Gold Layer: E-Commerce Aggregates

- **Area:** Data Storage — Medallion Architecture
- **Description:** The system must produce pre-aggregated, business-ready Gold Delta tables derived from Silver, optimized for BI dashboards and ML feature engineering.
- **Gold tables:**

  | Table | Description |
  |---|---|
  | `gold_daily_sales_summary` | Daily revenue, order count, and average order value — by product category and customer territory. |
  | `gold_product_performance` | Per-product totals: units sold, gross revenue, discount amount, average review rating. |
  | `gold_customer_rfm` | Customer RFM (Recency, Frequency, Monetary) scores and segments (Champion, Loyal, At-Risk, Lost). |
  | `gold_trending_products` | Ranked trending products by interaction count over configurable time windows (see BR-06). |

---

## BR-05 — Real-Time Product Interaction Feature Vector

- **Area:** Data Processing (T in ELT) — Streaming
- **Description:** To support real-time product recommendations, the system must process product interaction events in real-time using **Spark Structured Streaming** and produce a per-customer feature vector representing recent activity.
- **Use Case:** Customer 12345 views products A, B, and C within 5 minutes → the system immediately emits an updated feature vector so the recommendation model can respond in real-time.
- **Input:** Simulated streaming events with schema: `customer_id`, `product_id`, `interaction_type` (`view` | `add_to_cart` | `purchase`), `event_timestamp`.
- **Output feature vector per customer:**
  - `customer_id`
  - `recent_product_ids` — list of product IDs interacted with in the window.
  - `view_count`, `add_to_cart_count`, `purchase_count`
  - `window_start`, `window_end`
- **Windowing:** Sliding window of **5 minutes** (slide interval: 1 minute).
- **Sink:** Databricks Feature Store (online-serving enabled Delta table).

---

## BR-06 — Batch Trending Product Computation

- **Area:** Data Processing (T in ELT) — Batch
- **Description:** The system must compute a ranked list of trending/popular products based on aggregated e-commerce interactions, using batch processing. This serves as the baseline recommendation strategy (no real-time model needed).
- **Logic:** Aggregate `SalesOrderDetail`, `ProductReview`, and `ShoppingCartItem` interactions per product over configurable look-back windows (e.g., last 7 days, last 30 days). Rank products by total interaction count.
- **Output:** Written to `gold_trending_products` with columns: `product_id`, `product_name`, `category`, `total_interactions`, `rank`, `window_days`, `computed_at`.
- **Schedule:** Daily batch job (e.g., Databricks Workflow at midnight).

---

## BR-07 — E-Commerce Analytics Dashboard (BI)

- **Area:** Business Intelligence
- **Description:** The system must expose the Gold layer to a BI tool (e.g., Databricks SQL, Power BI) to answer the following **8 key business questions** about the e-commerce operation:

  | # | Business Question | Gold Table |
  |---|---|---|
  | Q1 | What is the monthly revenue trend, and which product categories drive the most sales? | `gold_daily_sales_summary` |
  | Q2 | What is the average order value (AOV) over time, and how does it vary by customer territory? | `gold_daily_sales_summary` |
  | Q3 | Which are the top 10 best-selling products by revenue and by units sold? | `gold_product_performance` |
  | Q4 | What is the impact of promotions/special offers on order volume and revenue? | `silver_special_offer` + `silver_sales_order` |
  | Q5 | What is the average product review rating per category, and which products have the lowest ratings? | `gold_product_performance` |
  | Q6 | What are the top purchase reasons driving online sales? | `silver_sales_order` (via `SalesReason`) |
  | Q7 | How are customers segmented by RFM, and what share falls into each segment? | `gold_customer_rfm` |
  | Q8 | What products are currently trending (last 7 days vs. last 30 days)? | `gold_trending_products` |

---

## BR-08 — Feature Store & Product Recommendation Serving

- **Area:** AI / ML
- **Description:** The system must provide a **Databricks Feature Store** to manage, version, and serve features for the product recommendation use case.
- **Feature store requirements:**
  - Store and version the real-time user interaction feature vector (from BR-05) and the customer RFM features (from `gold_customer_rfm`).
  - Support point-in-time lookups for offline model training.
  - Support online serving for real-time model inference.
  - Metadata management: feature descriptions, data types, owner, and freshness SLA.
  - Reference: [Databricks Feature Store](https://www.databricks.com/product/feature-store)
- **Recommendation serving — two strategies:**

  | Strategy | Trigger | Model Input | Use Case |
  |---|---|---|---|
  | **Real-time (BR-05)** | Per user request | Real-time interaction feature vector from Feature Store | Personalised "You may also like" |
  | **Batch trending (BR-06)** | Daily refresh | `gold_trending_products` | Cold-start users & "Trending Now" homepage section |

---

## Appendix: Data Source Summary

| Attribute | Value |
|---|---|
| **Source System** | AdventureWorks OLTP (SQL Server sample database) |
| **Format** | CSV (tab-delimited) |
| **Total CSV files ingested** | ~70 |
| **Downstream scope** | E-commerce domain (Sales, Product, Customer) |
| **Product Categories** | Bikes, Components, Clothing, Accessories |
| **Date Range** | ~2019–2025 |