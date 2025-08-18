1) Create & Load Tables
-- Clean-up if re-running
IF OBJECT_ID('dbo.FactSales') IS NOT NULL DROP TABLE dbo.FactSales;
IF OBJECT_ID('dbo.DimCustomer') IS NOT NULL DROP TABLE dbo.DimCustomer;

-- Dimension with 12 customers (IDs 101..112)
CREATE TABLE dbo.DimCustomer
(
    CustomerId   INT        NOT NULL PRIMARY KEY,
    [Name]       NVARCHAR(60) NOT NULL,
    [City]       NVARCHAR(40) NOT NULL,
    [State]      NVARCHAR(40) NOT NULL,
    Segment      NVARCHAR(20) NOT NULL -- e.g., Retail/Enterprise
);

INSERT INTO dbo.DimCustomer (CustomerId, [Name], [City], [State], Segment) VALUES
(101,'Aarav Shah','Mumbai','Maharashtra','Retail'),
(102,'Neha Verma','Pune','Maharashtra','Retail'),
(103,'Rohan Mehta','Delhi','Delhi','Enterprise'),
(104,'Isha Gupta','Bengaluru','Karnataka','Retail'),
(105,'Vikram Singh','Hyderabad','Telangana','Enterprise'),
(106,'Priya Nair','Kochi','Kerala','Retail'),
(107,'Kunal Joshi','Jaipur','Rajasthan','Retail'),
(108,'Ananya Das','Kolkata','West Bengal','Enterprise'),
(109,'Rahul Jain','Ahmedabad','Gujarat','Retail'),
(110,'Simran Kaur','Chandigarh','Chandigarh','Enterprise'),
(111,'Mohit Kapoor','Noida','Uttar Pradesh','Retail'),
(112,'Divya Iyer','Chennai','Tamil Nadu','Enterprise');
-- NOTE: No customer 113 or 999 in the dimension (missing keys to demo joins)

-- Fact table with 14 rows; includes some CustomerIds not present in the dimension
CREATE TABLE dbo.FactSales
(
    SalesId      INT         NOT NULL PRIMARY KEY,
    CustomerId   INT         NULL,          -- keep nullable to demo IS NULL cases if needed
    [SalesDate]  DATE        NOT NULL,
    Product      NVARCHAR(40) NOT NULL,
    Qty          INT         NOT NULL,
    UnitPrice    DECIMAL(18,2) NOT NULL,
    DiscountPct  DECIMAL(5,2)  NOT NULL      -- e.g., 5.00 = 5%
);

INSERT INTO dbo.FactSales (SalesId, CustomerId, [SalesDate], Product, Qty, UnitPrice, DiscountPct) VALUES
(1,  101, '2025-08-01', 'Fabric Pro',      2,  3500.00, 5.00),
(2,  102, '2025-08-01', 'Power BI Plus',   1,  1800.00, 0.00),
(3,  105, '2025-08-02', 'Fabric Pro',      3,  3500.00, 10.00),
(4,  109, '2025-08-03', 'Data Gateway',    5,   400.00, 0.00),
(5,  110, '2025-08-03', 'Fabric Pro',      1,  3500.00, 0.00),
(6,  112, '2025-08-04', 'PBIRS',           2,  2200.00, 5.00),
(7,  103, '2025-08-04', 'Power BI Plus',   4,  1800.00, 0.00),
(8,  108, '2025-08-05', 'OneLake Add-on',  6,   250.00, 0.00),
(9,  107, '2025-08-06', 'Fabric Pro',      1,  3500.00, 0.00),
(10, 113, '2025-08-06', 'Power BI Plus',   2,  1800.00, 0.00),  -- 113 is MISSING in Dim
(11, 999, '2025-08-07', 'Fabric Pro',      1,  3500.00, 15.00), -- 999 is MISSING in Dim
(12, 101, '2025-08-07', 'OneLake Add-on', 10,   250.00, 0.00),
(13, 105, '2025-08-08', 'Data Gateway',    2,   400.00, 0.00),
(14, 104, '2025-08-08', 'Fabric Pro',      2,  3500.00, 0.00);

2) Helpful calculated amount (line total)
-- Line Amount = Qty * UnitPrice * (1 - DiscountPct/100)
SELECT
    s.SalesId,
    s.CustomerId,
    s.[SalesDate],
    s.Product,
    s.Qty,
    s.UnitPrice,
    s.DiscountPct,
    CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2)) AS LineAmount
FROM dbo.FactSales AS s;


What it does: Shows how to compute net sales per row to use later in aggregations.

3) JOIN Examples
a) INNER JOIN (only matching keys)
SELECT
    s.SalesId, s.CustomerId, c.[Name], c.[City], c.[State], c.Segment,
    s.Product, s.Qty,
    CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2)) AS LineAmount
FROM dbo.FactSales AS s
INNER JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId;


What it does: Returns only sales where the customer exists in the dimension. Rows for CustomerId = 113/999 are excluded.

b) LEFT JOIN (keep all facts; dimension is optional)
SELECT
    s.SalesId, s.CustomerId, c.[Name], c.[City], c.[State], c.Segment,
    s.Product, s.Qty,
    CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2)) AS LineAmount
FROM dbo.FactSales AS s
LEFT JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId;


What it does: Keeps all fact rows, filling dimension columns with NULL when the key is missing (e.g., 113/999).

c) RIGHT JOIN (keep all dimensions; facts are optional)
SELECT
    s.SalesId, s.CustomerId, c.CustomerId AS DimCustomerId, c.[Name], c.[City], c.[State], c.Segment,
    s.Product, s.Qty
FROM dbo.FactSales AS s
RIGHT JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId;


What it does: Keeps all dimension members, even if they have no sales in the fact (e.g., a customer who never purchased).

d) FULL OUTER JOIN (see both kinds of gaps)
SELECT
    s.SalesId, s.CustomerId AS FactCustomerId,
    c.CustomerId AS DimCustomerId, c.[Name], c.[City], c.[State], c.Segment,
    s.Product, s.Qty
FROM dbo.FactSales AS s
FULL OUTER JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId
ORDER BY
    CASE WHEN s.CustomerId IS NULL THEN 1 ELSE 0 END,  -- dims with no facts first
    COALESCE(s.CustomerId, c.CustomerId), s.SalesId;


What it does: Shows both:

facts with missing dimension (dimension columns = NULL), and

dimensions with no facts (fact columns = NULL).

4) Filtering (WHERE), Grouping, and HAVING
a) WHERE filter (row-level before grouping)
-- Sales after 2025-08-03 and only for the 'Fabric Pro' product
SELECT
    s.SalesId, s.CustomerId, s.[SalesDate], s.Product, s.Qty
FROM dbo.FactSales AS s
WHERE s.[SalesDate] > '2025-08-03'
  AND s.Product = 'Fabric Pro';


What it does: Limits rows before any aggregation.

b) GROUP BY with aggregates
-- Total sales amount per customer (including customers missing in Dim via LEFT JOIN)
SELECT
    COALESCE(c.CustomerId, s.CustomerId) AS CustomerId,
    c.[Name],
    SUM(CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2))) AS TotalSales
FROM dbo.FactSales AS s
LEFT JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId
GROUP BY COALESCE(c.CustomerId, s.CustomerId), c.[Name]
ORDER BY TotalSales DESC;


What it does: Aggregates LineAmount per customer. COALESCE ensures orphan facts (e.g., 113/999) still show a CustomerId.

c) GROUP BY with HAVING (filter after aggregation)
-- Show customers (including unknowns) whose total sales exceed 5,000
SELECT
    COALESCE(c.CustomerId, s.CustomerId) AS CustomerId,
    COALESCE(c.[Name], '(Unknown Customer)') AS [Name],
    SUM(CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2))) AS TotalSales
FROM dbo.FactSales AS s
LEFT JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId
GROUP BY COALESCE(c.CustomerId, s.CustomerId), COALESCE(c.[Name], '(Unknown Customer)')
HAVING SUM(CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2))) > 5000
ORDER BY TotalSales DESC;


What it does: HAVING filters groups (post-aggregation). This keeps only customers whose TotalSales > 5000.

d) Finding missing keys explicitly
-- Facts that don't match any dim (orphans)
SELECT s.*
FROM dbo.FactSales AS s
LEFT JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId
WHERE c.CustomerId IS NULL;

-- Dim members with no sales
SELECT c.*
FROM dbo.DimCustomer AS c
LEFT JOIN dbo.FactSales AS s
    ON s.CustomerId = c.CustomerId
WHERE s.CustomerId IS NULL;


-------------- Insert more data and check time travel  

INSERT INTO dbo.FactSales (SalesId, CustomerId, [SalesDate], Product, Qty, UnitPrice, DiscountPct) VALUES
(15,  101, '2025-08-01', 'Fabric Pro',      2,  3500.00, 5.00);

-- Note time
SELECT GETDATE()

-- Change time in below query 
select * from dbo.FactSales OPTION (FOR TIMESTAMP AS OF '2025-08-18T02:25:35.28'); 


-----
What it does: Classic data-quality checks using LEFT JOIN + IS NULL.

5) Optional: Aggregate by attributes in the Dimension
-- State-wise total sales (rolls up facts by dim attributes)
SELECT
    c.[State],
    SUM(CAST(s.Qty * s.UnitPrice * (1 - s.DiscountPct/100.0) AS DECIMAL(18,2))) AS TotalSales
FROM dbo.FactSales AS s
INNER JOIN dbo.DimCustomer AS c
    ON s.CustomerId = c.CustomerId
GROUP BY c.[State]
ORDER BY TotalSales DESC;


What it does: Uses INNER JOIN so only sales with valid customers contribute to state totals.