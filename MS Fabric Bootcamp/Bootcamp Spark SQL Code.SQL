-- Welcome to your new notebook
-- Type here in the cell editor to add code!
CREATE TABLE IF NOT EXISTS sales (
  order_id         STRING,
  order_date       DATE,
  customer_id      STRING,
  product_id       STRING,
  qty              INT,
  unit_price       DOUBLE,
  discount_percent DOUBLE,
  gross_sales      DOUBLE
)

----
INSERT INTO sales VALUES
  ('O-1001', DATE'2025-08-01', 'C-01', 'P-10', 3, 199.99, 5.0, 3*199.99*(1-0.05)),
  ('O-1002', DATE'2025-08-02', 'C-02', 'P-11', 1, 499.00, 0.0, 499.00),
  ('O-1003', DATE'2025-08-03', 'C-03', 'P-12', 5, 99.50, 2.5, 5*99.50*(1-0.025)),
  ('O-1004', DATE'2025-08-04', 'C-04', 'P-13', 2, 249.75, 0.0, 2*249.75),
  ('O-1005', DATE'2025-08-05', 'C-05', 'P-14', 4, 120.00, 10.0, 4*120*(1-0.10)),
  ('O-1006', DATE'2025-08-06', 'C-06', 'P-15', 6, 75.00, 0.0, 6*75.00),
  ('O-1007', DATE'2025-08-07', 'C-07', 'P-16', 3, 350.00, 5.0, 3*350*(1-0.05)),
  ('O-1008', DATE'2025-08-08', 'C-08', 'P-17', 1, 999.99, 0.0, 999.99);
  
 -- 
 select * from sales
 
 ---
 
 UPDATE sales
SET discount_percent = 15.0,
    gross_sales = qty * unit_price * (1 - 15.0/100.0)
WHERE order_date BETWEEN DATE'2025-08-01' AND DATE'2025-08-04';

---

select * from sales

---

DESCRIBE HISTORY sales;

----

SELECT * FROM sales VERSION AS OF 1;

----

SELECT * FROM sales TIMESTAMP AS OF '2025-08-13T01:58:00Z';

----


DELETE FROM sales
WHERE qty <= 2;

----


select * from sales

----


DESCRIBE HISTORY sales;

---


RESTORE TABLE sales TO VERSION AS OF 2;

----


select * from sales

---

ALTER TABLE sales ADD COLUMN region varchar(50);

----


select * from sales

----


ALTER TABLE sales RENAME COLUMN product_id TO product_code;


----


INSERT INTO sales VALUES
  ('O-1009', DATE'2025-08-04', 'C-01', 'P-10', 3, 199.99, 4.0, 3*199.99*(1-0.04), 'North')
  
  ----
  
  
  INSERT INTO sales (order_id,order_date,customer_id,product_id,qty,unit_price,discount_percent,gross_sales) VALUES
  ('O-1010', DATE'2025-08-03', 'C-02', 'P-11', 3, 199.99, 4.0, 3*199.99*(1-0.04))
  
  ---
  
  
  CREATE OR REPLACE TABLE sales_copy
AS
SELECT * FROM sales;