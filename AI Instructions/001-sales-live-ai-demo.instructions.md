---
description: "Use when implementing the 001-Sales-Live-AI-Demo Microsoft Fabric medallion, Direct Lake semantic model, and report workflow."
applyTo: "{specs/001-sales-live-ai-demo/**,notebooks/sales-live-ai-demo/**,_scripts/*sales-live*,_pbip/SalesLiveAIDemo.Report/**}"
---

# 001-Sales-Live-AI-Demo Instructions

Use the Spec Kit documents in `/specs/001-sales-live-ai-demo/` as the controlling requirements before implementing this feature.

## Required Name

- Fabric workspace: `001-Sales-Live-AI-Demo`
- Bronze Lakehouse: `LH_Bronze_Sales`
- Silver Lakehouse: `LH_Silver_Sales`
- Gold Lakehouse: `LH_Gold_Sales`
- Pipeline: `PL_Sales_Live_AI_Demo`
- Semantic model: `SM_Sales_Live_AI_Demo`
- Report: `RP_Sales_Live_AI_Demo`

## Tool Constraints

- Use Microsoft Fabric MCP Server for Fabric workspace, Lakehouse, notebook, pipeline, OneLake, and report operations.
- Use Power BI Modeling MCP Server for Direct Lake semantic model tables, relationships, measures, and validation.
- Use Azure CLI only when needed for authentication, tenant, subscription, or session context. Tell the user before running any Azure CLI command.
- Do not use REST APIs through Command Prompt unless the user explicitly approves after being told why REST is needed.
- If a Fabric or Power BI operation is not available through MCP, stop and explain the blocker before using an alternate service path.

## Data Source

Source folder: `D:\Transfer Jun-2023\Amit Files\Google Drive\Kanerika\power bi\Data 2026\star_schema`

Canonical CSV sources observed:

- `DimCustomer.csv`
- `DimDate.csv`
- `DimOrderType.csv`
- `DimPaymentMethod.csv`
- `DimProduct.csv`
- `DimStore.csv`
- `FactSales.csv`

Treat `SalesDataStarSchema1M.xlsx` as a possible duplicate/cross-check until inspected.

## Implementation Order

1. Read `spec.md`, `plan.md`, `data-model.md`, and `tasks.md`.
2. Create or reuse workspace and three Lakehouses.
3. Load source files to Bronze.
4. Use PySpark notebooks for Silver and Gold transformations.
5. Generate new Gold surrogate keys and load facts using those surrogate keys.
6. Create and run the orchestration pipeline.
7. Create Direct Lake semantic model on Gold.
8. Add relationships and measures.
9. Create one test visual and validate it.
10. Create and publish the final three-page report.

## Semantic Model Rules

- Relationships must use Gold surrogate keys.
- Use single-direction filtering from dimensions to fact unless explicitly justified.
- Hide technical keys from report view when they are not needed by users.
- Include required measures from the spec before report authoring.

## Reporting Rules

- Build a test visual before the final report.
- Final report must have three pages: Executive Overview, Product & Customer Analysis, Store & Channel Performance.
- Use Fabric-inspired colors and generic logo assets.
- Do not use protected brand logos unless an approved local asset is provided.