/* =====================================================================
   WWI 02 · Silver → Gold (run in wh_gold, LAB-Platform-Gold-DEV)
   Adapted from Microsoft's "02 - Data Transformation - Business Aggregates".
   The original builds gold with Spark. Here gold lives in the Warehouse
   and reads lh_silver through a cross-database query in the same workspace.
   Prerequisite: in wh_gold Explorer → + Warehouses → add lh_silver.
   ===================================================================== */

-- Gold 1: sales by date and city
DROP TABLE IF EXISTS dbo.aggregate_sale_by_date_city;
CREATE TABLE dbo.aggregate_sale_by_date_city AS
SELECT
    d.[Date],
    d.CalendarMonthLabel,
    d.[Day],
    d.ShortMonth,
    d.CalendarYear,
    c.City,
    c.StateProvince,
    c.SalesTerritory,
    SUM(s.TotalExcludingTax)  AS SumOfTotalExcludingTax,
    SUM(s.TaxAmount)          AS SumOfTaxAmount,
    SUM(s.TotalIncludingTax)  AS SumOfTotalIncludingTax,
    SUM(s.Profit)             AS SumOfProfit
FROM lh_silver.dbo.fact_sale            AS s
JOIN lh_silver.dbo.dimension_date       AS d ON s.InvoiceDateKey = d.[Date]
JOIN lh_silver.dbo.dimension_city       AS c ON s.CityKey        = c.CityKey
GROUP BY d.[Date], d.CalendarMonthLabel, d.[Day], d.ShortMonth, d.CalendarYear,
         c.City, c.StateProvince, c.SalesTerritory;

-- Gold 2: sales by date and employee
DROP TABLE IF EXISTS dbo.aggregate_sale_by_date_employee;
CREATE TABLE dbo.aggregate_sale_by_date_employee AS
SELECT
    d.[Date],
    d.CalendarMonthLabel,
    d.[Day],
    d.ShortMonth            AS [Month],
    d.CalendarYear          AS [Year],
    e.PreferredName,
    e.Employee,
    SUM(s.TotalExcludingTax)  AS SumOfTotalExcludingTax,
    SUM(s.TaxAmount)          AS SumOfTaxAmount,
    SUM(s.TotalIncludingTax)  AS SumOfTotalIncludingTax,
    SUM(s.Profit)             AS SumOfProfit
FROM lh_silver.dbo.fact_sale            AS s
JOIN lh_silver.dbo.dimension_date       AS d ON s.InvoiceDateKey = d.[Date]
JOIN lh_silver.dbo.dimension_employee   AS e ON s.SalespersonKey = e.EmployeeKey
GROUP BY d.[Date], d.CalendarMonthLabel, d.[Day], d.ShortMonth, d.CalendarYear,
         e.PreferredName, e.Employee;

-- Check
SELECT TOP 10 City, SUM(SumOfProfit) AS Profit
FROM dbo.aggregate_sale_by_date_city
GROUP BY City
ORDER BY Profit DESC;
