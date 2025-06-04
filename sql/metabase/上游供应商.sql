-- PostgreSQL
-- 查询指定供应商的客户首次交易信息
-- 1. 定义目标供应商列表
-- 2. 统计每个客户针对每个供应商的首次交易日期
-- 3. 关联获取客户, 商品和供应商的基础信息

WITH "all_vendor" AS (
    SELECT 1002632 AS "vendor_id" UNION ALL
    SELECT 1002633 UNION ALL
    SELECT 1002560 UNION ALL
    SELECT 1002540 UNION ALL
    SELECT 1002322 UNION ALL
    SELECT 1003071 UNION ALL
    SELECT 1001793 UNION ALL
    SELECT 1003254 UNION ALL
    SELECT 1001858 UNION ALL
    SELECT 1003105 UNION ALL
    SELECT 1002875 UNION ALL
    SELECT 1002849 UNION ALL
    SELECT 1002807 UNION ALL
    SELECT 1002630 UNION ALL
    SELECT 1003322 UNION ALL
    SELECT 1003150
),
"first_transaction" AS (
    SELECT "dws"."sales_day_stats"."store_id",
           "dws"."sales_day_stats"."product_id",
           "dws"."sales_day_stats"."vendor_id",
           MIN("dws"."sales_day_stats"."settlement_day") AS "first_settlement_day"
      FROM "dws"."sales_day_stats"
     INNER JOIN "all_vendor"
             ON "dws"."sales_day_stats"."vendor_id" = "all_vendor"."vendor_id"
     WHERE "dws"."sales_day_stats"."settlement_day" >= '2022-01-01 00:00:00'::TIMESTAMP
       AND "dws"."sales_day_stats"."sales_value" <> 0
     GROUP BY 1, 2, 3
     ORDER BY 3, 1, 4
),
"final_result" AS (
    SELECT "first_transaction"."store_id" AS "客户编号",
          "live"."stores"."name" AS "客户名称",
          "first_transaction"."product_id" AS "商品ID",
          "live"."products"."name" AS "商品名称",
          "first_transaction"."vendor_id" AS "供应商ID",
          "live"."vendors"."name" AS "供应商名称",
          "first_transaction"."first_settlement_day"::DATE AS "首次成交结算日期"
      FROM "first_transaction"
    INNER JOIN "live"."stores"
            ON "first_transaction"."store_id" = "live"."stores"."id"
    INNER JOIN "live"."products"
            ON "first_transaction"."product_id" = "live"."products"."id"
    INNER JOIN "live"."vendors"
            ON "first_transaction"."vendor_id" = "live"."vendors"."id"
)
SELECT *
  FROM "final_result";