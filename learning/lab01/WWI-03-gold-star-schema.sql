/* =====================================================================
   Lab 01 · Stretch C: Gold as a Kimball star using the NTFB Gold standard
   Run in wh_gold (LAB-Platform-Gold-DEV). Reads lh_silver (added under + Warehouses).

   Standard applied:
   - dim. / fact. schemas, PascalCase names, no dim_/fact_ prefixes
   - Integer surrogate key <Entity>Key on every dimension
   - Business key <Entity>Id; SourceSystem + SourceRecId kept as lineage
   - SCD2 control columns EffectiveFrom / EffectiveTo / IsCurrent
     (IsCurrent = latest version per business key; this sample ends rows at 2025-01-01, not 9999-12-31)
   - Unknown member (Key = -1) so facts never drop rows on a failed lookup
   - Fact grain: one row per invoice line (WWI SaleKey)
   ===================================================================== */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')  EXEC('CREATE SCHEMA dim');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact') EXEC('CREATE SCHEMA fact');

/* ---------- dim.Date (smart key yyyymmdd) ---------- */
DROP TABLE IF EXISTS dim.[Date];
CREATE TABLE dim.[Date] AS
SELECT
    CAST(YEAR(d.[Date]) * 10000 + MONTH(d.[Date]) * 100 + DAY(d.[Date]) AS INT) AS DateKey,
    CAST(d.[Date] AS DATE)          AS [Date],
    d.[Day]                         AS DayOfMonth,
    d.[Month]                       AS MonthName,
    d.ShortMonth                    AS MonthShort,
    d.CalendarMonthNumber           AS MonthNumber,
    d.CalendarMonthLabel            AS MonthLabel,
    d.CalendarYear                  AS CalendarYear,
    d.FiscalMonthNumber             AS FiscalMonthNumber,
    d.FiscalYear                    AS FiscalYear,
    d.ISOWeekNumber                 AS IsoWeekNumber
FROM lh_silver.dbo.dimension_date AS d;

/* ---------- dim.City (SCD2) ---------- */
DROP TABLE IF EXISTS dim.City;
CREATE TABLE dim.City AS
SELECT CAST(ROW_NUMBER() OVER (ORDER BY c.CityKey) AS INT) AS CityKey,
       c.WWICityID          AS CityId,
       c.City, c.StateProvince, c.Country, c.SalesTerritory, c.Region,
       c.ValidFrom          AS EffectiveFrom,
       c.ValidTo            AS EffectiveTo,
       CAST(CASE WHEN c.ValidTo = MAX(c.ValidTo) OVER (PARTITION BY c.WWICityID) THEN 1 ELSE 0 END AS BIT) AS IsCurrent,
       'WWI'                AS SourceSystem,
       c.CityKey            AS SourceRecId
FROM lh_silver.dbo.dimension_city AS c
WHERE c.CityKey <> 0
UNION ALL
SELECT -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
       CAST('1900-01-01' AS DATETIME2(6)), CAST('9999-12-31' AS DATETIME2(6)), CAST(1 AS BIT), 'SYSTEM', 0;

/* ---------- dim.Customer (SCD2) ---------- */
DROP TABLE IF EXISTS dim.Customer;
CREATE TABLE dim.Customer AS
SELECT CAST(ROW_NUMBER() OVER (ORDER BY c.CustomerKey) AS INT) AS CustomerKey,
       c.WWICustomerID      AS CustomerId,
       c.Customer           AS CustomerName,
       c.BillToCustomer, c.Category, c.BuyingGroup, c.PostalCode,
       c.ValidFrom          AS EffectiveFrom,
       c.ValidTo            AS EffectiveTo,
       CAST(CASE WHEN c.ValidTo = MAX(c.ValidTo) OVER (PARTITION BY c.WWICustomerID) THEN 1 ELSE 0 END AS BIT) AS IsCurrent,
       'WWI'                AS SourceSystem,
       c.CustomerKey        AS SourceRecId
FROM lh_silver.dbo.dimension_customer AS c
WHERE c.CustomerKey <> 0
UNION ALL
SELECT -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
       CAST('1900-01-01' AS DATETIME2(6)), CAST('9999-12-31' AS DATETIME2(6)), CAST(1 AS BIT), 'SYSTEM', 0;

/* ---------- dim.Employee (SCD2) ---------- */
DROP TABLE IF EXISTS dim.Employee;
CREATE TABLE dim.Employee AS
SELECT CAST(ROW_NUMBER() OVER (ORDER BY e.EmployeeKey) AS INT) AS EmployeeKey,
       e.WWIEmployeeID      AS EmployeeId,
       e.Employee           AS EmployeeName,
       e.PreferredName,
       e.IsSalesperson,
       e.ValidFrom          AS EffectiveFrom,
       e.ValidTo            AS EffectiveTo,
       CAST(CASE WHEN e.ValidTo = MAX(e.ValidTo) OVER (PARTITION BY e.WWIEmployeeID) THEN 1 ELSE 0 END AS BIT) AS IsCurrent,
       'WWI'                AS SourceSystem,
       e.EmployeeKey        AS SourceRecId
FROM lh_silver.dbo.dimension_employee AS e
WHERE e.EmployeeKey <> 0
UNION ALL
SELECT -1, -1, 'Unknown', 'Unknown', CAST(0 AS BIT),
       CAST('1900-01-01' AS DATETIME2(6)), CAST('9999-12-31' AS DATETIME2(6)), CAST(1 AS BIT), 'SYSTEM', 0;

/* ---------- dim.StockItem (SCD2) ---------- */
DROP TABLE IF EXISTS dim.StockItem;
CREATE TABLE dim.StockItem AS
SELECT CAST(ROW_NUMBER() OVER (ORDER BY s.StockItemKey) AS INT) AS StockItemKey,
       s.WWIStockItemID     AS StockItemId,
       s.StockItem          AS StockItemName,
       s.Color, s.Brand, s.Size, s.IsChillerStock, s.UnitPrice,
       s.ValidFrom          AS EffectiveFrom,
       s.ValidTo            AS EffectiveTo,
       CAST(CASE WHEN s.ValidTo = MAX(s.ValidTo) OVER (PARTITION BY s.WWIStockItemID) THEN 1 ELSE 0 END AS BIT) AS IsCurrent,
       'WWI'                AS SourceSystem,
       s.StockItemKey       AS SourceRecId
FROM lh_silver.dbo.dimension_stock_item AS s
WHERE s.StockItemKey <> 0
UNION ALL
SELECT -1, -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', CAST(0 AS BIT), NULL,
       CAST('1900-01-01' AS DATETIME2(6)), CAST('9999-12-31' AS DATETIME2(6)), CAST(1 AS BIT), 'SYSTEM', 0;

/* ---------- fact.Sale (grain: one invoice line) ---------- */
DROP TABLE IF EXISTS fact.Sale;
CREATE TABLE fact.Sale AS
SELECT
    CAST(YEAR(f.InvoiceDateKey) * 10000 + MONTH(f.InvoiceDateKey) * 100 + DAY(f.InvoiceDateKey) AS INT) AS InvoiceDateKey,
    COALESCE(c.CityKey,     -1) AS CityKey,
    COALESCE(cu.CustomerKey, -1) AS CustomerKey,
    COALESCE(e.EmployeeKey, -1) AS SalespersonKey,
    COALESCE(si.StockItemKey, -1) AS StockItemKey,
    f.WWIInvoiceID              AS InvoiceId,          -- degenerate dimension
    f.Quantity,
    f.UnitPrice,
    f.TotalExcludingTax,
    f.TaxAmount,
    f.TotalIncludingTax,
    f.Profit,
    'WWI'                       AS SourceSystem,
    f.SaleKey                   AS SourceRecId
FROM lh_silver.dbo.fact_sale AS f
LEFT JOIN dim.City      AS c  ON c.SourceRecId  = f.CityKey
LEFT JOIN dim.Customer  AS cu ON cu.SourceRecId = f.CustomerKey
LEFT JOIN dim.Employee  AS e  ON e.SourceRecId  = f.SalespersonKey
LEFT JOIN dim.StockItem AS si ON si.SourceRecId = f.StockItemKey;

/* ---------- Checks ---------- */
-- 1. Row counts: fact.Sale must equal silver
SELECT 'silver' AS Layer, COUNT(*) AS Rows FROM lh_silver.dbo.fact_sale
UNION ALL SELECT 'gold', COUNT(*) FROM fact.Sale;

-- 2. Unknown-member hits per dimension (should be 0 or explainable)
SELECT SUM(CASE WHEN CityKey = -1 THEN 1 ELSE 0 END)        AS UnknownCity,
       SUM(CASE WHEN CustomerKey = -1 THEN 1 ELSE 0 END)    AS UnknownCustomer,
       SUM(CASE WHEN SalespersonKey = -1 THEN 1 ELSE 0 END) AS UnknownSalesperson,
       SUM(CASE WHEN StockItemKey = -1 THEN 1 ELSE 0 END)   AS UnknownStockItem
FROM fact.Sale;

-- 3. Star query: profit by territory and year
SELECT d.CalendarYear, c.SalesTerritory, SUM(s.Profit) AS Profit
FROM fact.Sale s
JOIN dim.[Date] d ON d.DateKey = s.InvoiceDateKey
JOIN dim.City   c ON c.CityKey = s.CityKey
GROUP BY d.CalendarYear, c.SalesTerritory
ORDER BY d.CalendarYear, Profit DESC;
