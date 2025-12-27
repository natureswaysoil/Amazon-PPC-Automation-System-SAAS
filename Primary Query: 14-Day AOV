-- Optimized AOV Calculation
-- 1. Looks back 60 days for better statistical significance.
-- 2. Excludes last 3 days to avoid "Attribution Lag" (incomplete data).
-- 3. Filters out low-data outliers.

SELECT
    advertisedAsin AS asin,
    SAFE_DIVIDE(SUM(sales), NULLIF(SUM(orders), 0)) AS aov,
    SUM(orders) AS orders,
    SUM(sales) AS sales,
    COUNT(DISTINCT segments_date) AS active_days
FROM `amazon-ppc-474902.amazon_ppc.sp_advertised_product_metrics`
WHERE 
    -- FIX: Look back 60 days, but STOP 3 days ago
    segments_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 63 DAY) 
                      AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND sales > 0 
GROUP BY asin
HAVING 
    orders >= 3  -- FIX: Increased threshold (2 is statistically weak)
    AND aov > 5  -- FIX: Lowered sanity check to catch low-ticket items properly
ORDER BY orders DESC;
