/* Create Tables 

drop table [dbo].[dim_item]

-- Create the SCD Type 2 item dimension table
CREATE TABLE [dbo].[dim_item](
    [Item_SurrogateKey] uniqueidentifier NOT NULL,
    [Item_Id] [int] NOT NULL,
    [Name] [varchar](50) NOT NULL,
    [Brand_Id] [int] NOT NULL,
    [Category_Id] [int] NOT NULL,
    [Sub_Category_Id] [int] NOT NULL,
    [Brand] [varchar](50) NOT NULL,
    [Category] [varchar](50) NOT NULL,
    [Sub_Category] [varchar](50) NOT NULL,
    [Valid_Start_Date] [datetime2](6) NOT NULL,
    [Valid_End_Date] [datetime2](6) NULL
)


CREATE TABLE [dbo].[sales_fact](
	[Order_No] [int] NOT NULL,
	[Item_ID] [int] NOT NULL,
	[Sales_Date] [datetime2](7) NOT NULL,
	[Customer_Id] [int] NOT NULL,
	[City_Id] [int] NOT NULL,
	[Qty] [decimal](18, 10) NOT NULL,
	[Price] [decimal](18, 10) NOT NULL,
	[COGS] [decimal](18, 10) NOT NULL,
	[Discount_Percent] [decimal](18, 10) NOT NULL,
	[Item_SurrogateKey] uniqueidentifier 
)

Get data from https://github.com/amitchandakpbi/powerbi/raw/main/Sales%20Data%20Used%20in%20Video.xlsx

*/

CREATE PROCEDURE UpdateDimItemAndInsertSalesFact
AS
BEGIN
    -- Declare a variable to hold the current date and time
    DECLARE @CurrentDateTime DATETIME;
    SET @CurrentDateTime = GETDATE();

    -- Update existing records with the current date and time
    UPDATE t
    SET t.[Valid_End_Date] = @CurrentDateTime
    FROM [dbo].[dim_item] AS t
    INNER JOIN [dbo].[item_stg] AS s ON t.[Item_Id] = s.[Item_Id]
    WHERE t.[Valid_End_Date] IS NULL
        AND (
            t.[Name] <> s.[Name]
            OR t.[Brand_Id] <> s.[Brand_Id]
            OR t.[Category_Id] <> s.[Category_Id]
            OR t.[Sub_Category_Id] <> s.[Sub_Category_Id]
            OR t.[Brand] <> s.[Brand]
            OR t.[Category] <> s.[Category]
            OR t.[Sub_Category] <> s.[Sub_Category]
        );

    -- Insert new records into the target table for changed data
    INSERT INTO [dbo].[dim_item](
        [Item_Id],
        [Name],
        [Brand_Id],
        [Category_Id],
        [Sub_Category_Id],
        [Brand],
        [Category],
        [Sub_Category],
        [Valid_Start_Date],
        [Valid_End_Date]
    )
    SELECT 
        s.[Item_Id],
        s.[Name],
        s.[Brand_Id],
        s.[Category_Id],
        s.[Sub_Category_Id],
        s.[Brand],
        s.[Category],
        s.[Sub_Category],
        CASE WHEN (SELECT COUNT(*) FROM [dbo].[dim_item] t1 WHERE t1.[Item_Id] = s.[Item_Id]) <= 0 THEN '2018-01-01' ELSE @CurrentDateTime END AS [Valid_Start_Date],
        NULL AS [Valid_End_Date]
    FROM [dbo].[dim_item] AS t
    RIGHT JOIN [dbo].[item_stg] AS s ON (t.[Item_Id] = s.[Item_Id]
        AND t.[Name] = s.[Name]
        AND t.[Brand_Id] = s.[Brand_Id]
        AND t.[Category_Id] = s.[Category_Id]
        AND t.[Sub_Category_Id] = s.[Sub_Category_Id]
        AND t.[Brand] = s.[Brand]
        AND t.[Category] = s.[Category]
        AND t.[Sub_Category] = s.[Sub_Category])
    WHERE t.[Item_Id] IS NULL;

    -- Insert into sales_fact table
    INSERT INTO [dbo].[sales_fact] (
        [Order_No],
        [Item_ID],
        [Sales_Date],
        [Customer_Id],
        [City_Id],
        [Qty],
        [Price],
        [COGS],
        [Discount_Percent],
        [Item_SurrogateKey]
    )
    SELECT
        stg.[Order_No],
        stg.[Item_ID],
        stg.[Sales_Date],
        stg.[Customer_Id],
        stg.[City_Id],
        stg.[Qty],
        stg.[Price],
        stg.[COGS],
        stg.[Discount_Percent],
        dim.[Item_SurrogateKey]
    FROM
        [dbo].[sales_fact_stg] AS stg
    JOIN
        [dbo].[dim_item] AS dim
    ON
        stg.[Item_ID] = dim.[Item_Id]
        AND stg.[Sales_Date] >= dim.[Valid_Start_Date] AND stg.[Sales_Date] < COALESCE(dim.[Valid_End_Date], '9999-12-31 23:59:59.9999999');
END;
