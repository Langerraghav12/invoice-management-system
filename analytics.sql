-- ============================================================
-- Invoice Management System — Advanced SQL Analytics
-- Author: Rahul | Bengaluru, India
-- Purpose: Demonstrates SQL skills for Oracle internship
-- Covers: JOINs, Aggregates, Window Functions,
--         Views, Stored Procedures, Triggers, Indexes
-- ============================================================

USE invoicemgsys;

-- ============================================================
-- SECTION 1: VIEWS — Reusable business logic
-- ============================================================

-- View 1: Full invoice summary with customer details
CREATE OR REPLACE VIEW vw_invoice_summary AS
SELECT
    i.invoice        AS invoice_no,
    i.invoice_date,
    i.invoice_due_date,
    i.status,
    i.invoice_type,
    c.name           AS customer_name,
    c.email          AS customer_email,
    c.town           AS customer_city,
    c.county         AS customer_country,
    i.subtotal,
    i.shipping,
    i.discount,
    i.vat,
    i.total
FROM invoices i
JOIN customers c ON i.invoice = c.invoice;

-- View 2: Product revenue report
CREATE OR REPLACE VIEW vw_product_revenue AS
SELECT
    p.product_name,
    p.product_price                         AS unit_price,
    COUNT(ii.id)                            AS times_ordered,
    SUM(ii.qty)                             AS total_qty_sold,
    SUM(CAST(ii.subtotal AS DECIMAL(10,2))) AS total_revenue
FROM products p
LEFT JOIN invoice_items ii ON ii.product LIKE CONCAT(p.product_name, '%')
GROUP BY p.product_id, p.product_name, p.product_price;

-- View 3: Customer lifetime value
CREATE OR REPLACE VIEW vw_customer_ltv AS
SELECT
    sc.id,
    sc.name,
    sc.email,
    sc.town,
    COUNT(i.id)    AS total_invoices,
    SUM(i.total)   AS lifetime_value,
    AVG(i.total)   AS avg_invoice_value,
    MAX(i.total)   AS largest_invoice,
    MIN(i.total)   AS smallest_invoice
FROM store_customers sc
JOIN customers c  ON sc.email = c.email
JOIN invoices  i  ON c.invoice = i.invoice
GROUP BY sc.id, sc.name, sc.email, sc.town;

-- View 4: Outstanding (unpaid) invoices with aging
CREATE OR REPLACE VIEW vw_outstanding_invoices AS
SELECT
    i.invoice       AS invoice_no,
    c.name          AS customer_name,
    c.email,
    i.invoice_date,
    i.invoice_due_date,
    i.total         AS amount_due,
    DATEDIFF(CURDATE(), STR_TO_DATE(i.invoice_due_date, '%d/%m/%Y')) AS days_overdue
FROM invoices i
JOIN customers c ON i.invoice = c.invoice
WHERE i.status != 'paid'
ORDER BY days_overdue DESC;


-- ============================================================
-- SECTION 2: ANALYTICAL QUERIES — Window Functions
-- ============================================================

-- Query 1: Rank customers by total spend (window function)
SELECT
    sc.name,
    SUM(i.total)                                        AS total_spend,
    RANK() OVER (ORDER BY SUM(i.total) DESC)            AS spend_rank,
    DENSE_RANK() OVER (ORDER BY SUM(i.total) DESC)      AS dense_rank,
    ROUND(
        SUM(i.total) * 100.0 / SUM(SUM(i.total)) OVER (),
        2
    )                                                   AS pct_of_total_revenue
FROM store_customers sc
JOIN customers c ON sc.email = c.email
JOIN invoices  i ON c.invoice = i.invoice
GROUP BY sc.id, sc.name;

-- Query 2: Running total of revenue over time
SELECT
    invoice_date,
    total,
    SUM(total) OVER (
        ORDER BY STR_TO_DATE(invoice_date, '%d/%m/%Y')
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM invoices
ORDER BY STR_TO_DATE(invoice_date, '%d/%m/%Y');

-- Query 3: Month-over-month revenue comparison
SELECT
    DATE_FORMAT(STR_TO_DATE(invoice_date, '%d/%m/%Y'), '%Y-%m') AS month,
    SUM(total)                                                   AS monthly_revenue,
    LAG(SUM(total)) OVER (
        ORDER BY DATE_FORMAT(STR_TO_DATE(invoice_date, '%d/%m/%Y'), '%Y-%m')
    )                                                            AS prev_month_revenue,
    SUM(total) - LAG(SUM(total)) OVER (
        ORDER BY DATE_FORMAT(STR_TO_DATE(invoice_date, '%d/%m/%Y'), '%Y-%m')
    )                                                            AS revenue_change
FROM invoices
GROUP BY month
ORDER BY month;

-- Query 4: Top N products per revenue (using window function)
SELECT *
FROM (
    SELECT
        ii.product,
        SUM(CAST(ii.subtotal AS DECIMAL(10,2)))        AS product_revenue,
        ROW_NUMBER() OVER (
            ORDER BY SUM(CAST(ii.subtotal AS DECIMAL(10,2))) DESC
        )                                              AS revenue_rank
    FROM invoice_items ii
    GROUP BY ii.product
) ranked
WHERE revenue_rank <= 5;

-- Query 5: Invoice status breakdown with percentage
SELECT
    status,
    COUNT(*)                                AS invoice_count,
    SUM(total)                              AS total_value,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        1
    )                                       AS pct_of_invoices
FROM invoices
GROUP BY status;


-- ============================================================
-- SECTION 3: COMPLEX JOINS & SUBQUERIES
-- ============================================================

-- Query 6: Customers with no invoices (LEFT JOIN + NULL check)
SELECT
    sc.id,
    sc.name,
    sc.email,
    sc.town
FROM store_customers sc
LEFT JOIN customers c ON sc.email = c.email
WHERE c.id IS NULL;

-- Query 7: Products never ordered
SELECT
    p.product_id,
    p.product_name,
    p.product_price
FROM products p
WHERE p.product_name NOT IN (
    SELECT DISTINCT SUBSTRING_INDEX(product, ' - ', 1)
    FROM invoice_items
);

-- Query 8: Invoices above average value (correlated subquery)
SELECT
    i.invoice,
    c.name AS customer,
    i.total,
    i.status
FROM invoices i
JOIN customers c ON i.invoice = c.invoice
WHERE i.total > (SELECT AVG(total) FROM invoices)
ORDER BY i.total DESC;

-- Query 9: Full invoice detail with all line items (multi-table JOIN)
SELECT
    i.invoice         AS invoice_no,
    i.invoice_date,
    i.status,
    c.name            AS billed_to,
    c.town,
    ii.product,
    ii.qty,
    ii.price          AS unit_price,
    ii.discount       AS item_discount,
    ii.subtotal       AS line_total,
    i.shipping,
    i.vat,
    i.total           AS invoice_total
FROM invoices i
JOIN customers     c  ON i.invoice = c.invoice
JOIN invoice_items ii ON i.invoice = ii.invoice
ORDER BY i.invoice, ii.id;


-- ============================================================
-- SECTION 4: STORED PROCEDURES
-- ============================================================

DELIMITER $$

-- Procedure 1: Get full invoice report for a given invoice number
CREATE PROCEDURE sp_get_invoice_report(IN p_invoice VARCHAR(50))
BEGIN
    -- Header
    SELECT
        i.invoice, i.invoice_date, i.invoice_due_date,
        i.status, i.subtotal, i.shipping, i.discount, i.vat, i.total,
        c.name, c.email, c.address_1, c.town, c.county, c.postcode
    FROM invoices i
    JOIN customers c ON i.invoice = c.invoice
    WHERE i.invoice = p_invoice;

    -- Line items
    SELECT product, qty, price, discount, subtotal
    FROM invoice_items
    WHERE invoice = p_invoice;
END$$

-- Procedure 2: Revenue summary for a date range
CREATE PROCEDURE sp_revenue_summary(
    IN p_start_date VARCHAR(20),
    IN p_end_date   VARCHAR(20)
)
BEGIN
    SELECT
        COUNT(*)        AS total_invoices,
        SUM(total)      AS gross_revenue,
        SUM(discount)   AS total_discounts,
        SUM(vat)        AS total_tax,
        SUM(shipping)   AS total_shipping,
        AVG(total)      AS avg_invoice_value,
        SUM(CASE WHEN status = 'paid' THEN total ELSE 0 END) AS collected,
        SUM(CASE WHEN status != 'paid' THEN total ELSE 0 END) AS outstanding
    FROM invoices
    WHERE STR_TO_DATE(invoice_date, '%d/%m/%Y')
          BETWEEN STR_TO_DATE(p_start_date, '%d/%m/%Y')
              AND STR_TO_DATE(p_end_date,   '%d/%m/%Y');
END$$

-- Procedure 3: Mark invoice as paid
CREATE PROCEDURE sp_mark_invoice_paid(IN p_invoice VARCHAR(50))
BEGIN
    UPDATE invoices
    SET status = 'paid'
    WHERE invoice = p_invoice;

    SELECT ROW_COUNT() AS rows_updated;
END$$

DELIMITER ;


-- ============================================================
-- SECTION 5: TRIGGERS — Audit logging
-- ============================================================

-- Audit log table
CREATE TABLE IF NOT EXISTS invoice_audit_log (
    log_id      INT AUTO_INCREMENT PRIMARY KEY,
    invoice_no  VARCHAR(50),
    old_status  VARCHAR(50),
    new_status  VARCHAR(50),
    changed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

-- Trigger: Log every time an invoice status changes
CREATE TRIGGER trg_invoice_status_change
AFTER UPDATE ON invoices
FOR EACH ROW
BEGIN
    IF OLD.status != NEW.status THEN
        INSERT INTO invoice_audit_log (invoice_no, old_status, new_status)
        VALUES (NEW.invoice, OLD.status, NEW.status);
    END IF;
END$$

DELIMITER ;


-- ============================================================
-- SECTION 6: INDEXES — Performance optimization
-- ============================================================

-- Speed up invoice lookups by status
CREATE INDEX idx_invoices_status ON invoices(status);

-- Speed up joining customers to invoices
CREATE INDEX idx_customers_invoice ON customers(invoice);

-- Speed up joining invoice_items to invoices
CREATE INDEX idx_invoice_items_invoice ON invoice_items(invoice);

-- Speed up customer search by email
CREATE INDEX idx_store_customers_email ON store_customers(email);

-- Composite index for date-range revenue queries
CREATE INDEX idx_invoices_date_status ON invoices(invoice_date, status);


-- ============================================================
-- SECTION 7: SCHEMA IMPROVEMENTS (FK refactor suggestion)
-- ============================================================
-- Note: Current schema uses MyISAM (no FK support).
-- Recommended: migrate to InnoDB for referential integrity.

-- Example of how invoice_items should reference invoices with FK:
-- ALTER TABLE invoice_items
--     ADD CONSTRAINT fk_items_invoice
--     FOREIGN KEY (invoice) REFERENCES invoices(invoice)
--     ON DELETE CASCADE;

-- This would prevent orphaned line items when invoices are deleted.
