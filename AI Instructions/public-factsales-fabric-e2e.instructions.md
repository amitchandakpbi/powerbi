---
description: "Use to build an end-to-end Microsoft Fabric medallion, Direct Lake semantic model, and Power BI report from the public BIWorld star-schema CSV files."
applyTo: "{specs/public-factsales-fabric-e2e/**,notebooks/public-factsales-fabric-e2e/**,_scripts/*public-factsales*,_pbip/PublicFactSales.Report/**}"
---

# Public BIWorld Sales Fabric E2E Instructions

Build a complete Microsoft Fabric demo from the public BIWorld data files. The directory cannot be listed directly, so use known file names appended to the base URL.

- Base URL: `https://biworld.in/data/`
- Files:
   - `https://biworld.in/data/DimCustomer.csv`
   - `https://biworld.in/data/DimDate.csv`
   - `https://biworld.in/data/DimOrderType.csv`
   - `https://biworld.in/data/DimPaymentMethod.csv`
   - `https://biworld.in/data/DimProduct.csv`
   - `https://biworld.in/data/DimStore.csv`
   - `https://biworld.in/data/FactSales.csv`
   - Optional cross-check bundle: `https://biworld.in/data/SalesDataStarSchema1M.xlsx`

The fact CSV contains fact grain and natural keys:

- `SalesKey`, `OrderNo`, `OrderDateKey`, `DeliveryDateKey`, `ProductKey`, `CustomerKey`, `StoreKey`, `PaymentMethodKey`, `OrderTypeKey`
- `Qty`, `UnitPrice`, `UnitCost`, `DiscountPct`, `DiscountAmt`, `GrossSales`, `NetSales`, `COGS`, `Margin`

## Required Names

- Workspace: `Public-FactSales-Fabric-Demo`
- Bronze Lakehouse: `LH_Bronze_FactSales`
- Silver Lakehouse: `LH_Silver_FactSales`
- Gold Lakehouse: `LH_Gold_FactSales`
- Pipeline: `PL_Public_FactSales_Demo`
- Semantic model: `SM_Public_FactSales_Demo`
- Report: `RP_Public_FactSales_Demo`

## Tool Rules

- Prefer Microsoft Fabric MCP Server for workspace, Lakehouse, notebook, pipeline, OneLake, and report operations.
- Prefer Power BI Modeling MCP Server for Direct Lake model tables, relationships, measures, and validation.
- Use Azure CLI only for authentication or token/session context, and tell the user before running it.
- If REST is required because MCP does not expose an operation, explain why first, then use Fabric REST with `https://api.fabric.microsoft.com` and poll LROs to `Succeeded`.

## End-to-End Flow

1. Create or reuse the workspace and assign capacity if Lakehouse creation requires it.
2. Create Bronze, Silver, and Gold Lakehouses.
3. Land all seven CSV files into Bronze Files first. Do not depend on Spark reading external HTTP URLs directly; download once, then read from Lakehouse Files.
4. Bronze notebook: read each CSV as strings/loose schema, add `source_file`, `ingestion_timestamp_utc`, and `batch_id`, then write Bronze Delta tables for all source files.
5. Silver notebook: cast all keys to integers, dates from `yyyyMMdd` or ISO date strings, booleans to booleans, amounts to decimals, and write clean Silver Delta tables.
6. Gold notebook: build dimensions from the public dimension CSVs, not from fact-only distinct keys:
   - `DimDateGold` from `DimDate.csv`
   - `DimProductGold` from `DimProduct.csv`
   - `DimCustomerGold` from `DimCustomer.csv`
   - `DimStoreGold` from `DimStore.csv`
   - `DimPaymentMethodGold` from `DimPaymentMethod.csv`
   - `DimOrderTypeGold` from `DimOrderType.csv`
7. Generate deterministic surrogate keys in Gold dimensions, then write `FactSalesGold` from `FactSales.csv` with surrogate foreign keys plus sales metrics.
8. Create a pipeline that runs Bronze, Silver, Gold, and validation notebooks in order.
9. Create Direct Lake semantic model on the Gold Lakehouse.
10. Add relationships from dimensions to fact using Gold surrogate keys. Use Order Date as active; Delivery Date may be inactive/secondary.
11. Add measures: `Net Sales`, `Gross Sales`, `COGS`, `Margin`, `Margin %`, `Quantity`, `Order Count`, `Average Order Value`, `Discount Amount`, `Discount %`, `Customer Count`, `Product Count`, `Sales YoY`, `Sales YoY %`.
12. Build and publish one simple test visual first. Continue only after it renders in Fabric.
13. Build final report with three pages:
   - Executive Overview
   - Product & Customer Analysis
   - Store & Channel Performance

## Report Authoring Lessons Learned

- Always validate PBIR with `powerbi-report-author validate <ReportFolder> --pretty` before publishing.
- Use `definition/version.json` value `2.0.0`, `definition.pbir` version `4.0`, page schema `page/2.1.0`, and visual schema `visualContainer/2.9.0` unless starting from an existing report with different schemas.
- Include `.platform`; Fabric may accept uploaded parts without rendering correctly if required PBIR metadata is wrong.
- Use 20-character lowercase hex visual IDs for visual folder names and `visual.name`.
- For `cardVisual`, the role is `Data`, not `Values` or `Fields`.
- Include `queryRef` and `nativeQueryRef` in every projection.
- Publish all report definition parts every time; `updateDefinition` replaces the full report definition.
- Use `getDefinition?format=PBIR` when downloading or inspecting a report definition.

## Notebook/Lakehouse Lessons Learned

- Notebook `known_lakehouses` metadata must be an array of objects such as `{ id = '<lakehouse-id>' }`, not raw string IDs.
- OneLake file paths should include the `Files/` prefix when uploading or listing files through API tooling.
- If creating schema-enabled Lakehouses is required and MCP does not expose the option, use Fabric REST creation payload with `creationPayload.enableSchemas = true` after explaining the fallback.

## Validation Checklist

- All seven public CSV files are landed in Bronze Files and Bronze Delta tables exist.
- Silver table has typed columns and no unexpected nulls in required keys.
- Gold dimensions have surrogate keys and `FactSalesGold` resolves all dimension keys.
- Pipeline exists and can run notebooks in dependency order.
- Direct Lake semantic model returns non-empty KPI DAX results.
- Test report visual renders before building the final report.
- Final report has all three pages and no broken visuals in Fabric.