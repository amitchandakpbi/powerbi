
// Use "take" to view a sample number of records in the table and check the data.
Sales
| take 100

Sales

// 2. Multiple Filters
Sales
| where Region == "East" and Category == "Watches" and Qty > 1
// See how many records are in the table.

// 3. Group By with Aggregation
Sales
| summarize TotalSales = sum(Qty*Unit_Price), TotalQty = sum(Qty) by Category

Sales
| count

// 4. Trending Analysis
Sales
| where Order_Date > ago(3600d)
| summarize DailySales = sum(Qty*Unit_Price) by bin(Order_Date, 1d)
| order by Order_Date asc


Sales
| where Order_Date > ago(3600d)
| summarize DailySales = sum(Qty*Unit_Price) by bin(Order_Date, 7d)
| order by Order_Date asc

// 5. Calculate Discounts and Gross Margins
Sales
| extend GrossMargin = (Qty*Unit_Price)  - (Qty * Unit_Cost)
| summarize TotalSales = sum(Qty*Unit_Price), TotalGrossMargin = sum(GrossMargin) by Brand

// 6. Top N Items by Sales
Sales
| summarize TotalSales = sum(Qty*Unit_Price) by Brand, Category
| top 5 by TotalSales desc

Sales | project  Brand, Category, City, Gross = (Qty*Unit_Price) 