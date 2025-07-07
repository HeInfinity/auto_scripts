-- PostgreSQL
-- 品类销售数据聚合分析

WITH "date_range" AS (
     SELECT '2025-06-01'::DATE AS "start_date",
			'2025-06-16'::DATE AS "end_date"
),

"base_data" AS (
    SELECT "live"."dwd"."delivercenter_info"."sub_name",
           "live"."dws"."category_sales_month_stats"."sales_month",
           "live"."dwd"."category_info"."first_name" AS "first_category_name",
           "live"."dwd"."category_info"."second_name" AS "second_category_name",
           "live"."dwd"."category_info"."name" AS "third_category_name",
					 "live"."dws"."category_sales_month_stats"."spu_id",
           "live"."dws"."category_sales_month_stats"."net_sales",
           "live"."dws"."category_sales_month_stats"."net_profit",
           "live"."dws"."category_sales_month_stats"."store_ids",
           "live"."dws"."category_sales_month_stats"."net_ud_quantity"
      FROM "live"."dws"."category_sales_month_stats"
     INNER JOIN "live"."dwd"."category_info"
             ON "live"."dws"."category_sales_month_stats"."spu_id" = "live"."dwd"."category_info"."id"
     INNER JOIN "live"."dwd"."delivercenter_info"
             ON "live"."dws"."category_sales_month_stats"."dc_id" = "live"."dwd"."delivercenter_info"."id"
		 CROSS JOIN "date_range"
     WHERE "live"."dwd"."delivercenter_info"."regional_id" IN (1, 2, 5, 8)
       AND "live"."dws"."category_sales_month_stats"."sales_month" >= "date_range"."start_date"
			 AND "live"."dws"."category_sales_month_stats"."sales_month" <= "date_range"."end_date"
),

"aggregated_data" AS (
    SELECT "base_data"."sub_name" AS "分子公司",
           "base_data"."sales_month" AS "结算月份",
           "base_data"."first_category_name" AS "一级品类",
           "base_data"."second_category_name" AS "二级品类", 
           "base_data"."third_category_name" AS "三级品类",
					 "base_data"."spu_id" AS "三级品类ID",
           SUM("base_data"."net_sales") AS "结算净产值",
           SUM("base_data"."net_profit") AS "结算净毛利",
           SUM("base_data"."net_ud_quantity") AS "结算净销量",
           SUM(array_length("base_data"."store_ids", 1)) AS "下单客户数",
           CASE 
           WHEN SUM("base_data"."net_ud_quantity") > 0 THEN 
               (SUM("base_data"."net_sales") - SUM("base_data"."net_profit")) / SUM("base_data"."net_ud_quantity")
           ELSE 0 
           END AS "成本单价"
      FROM "base_data"
     GROUP BY 1, 2, 3, 4, 5, 6
     ORDER BY 2, 1, 6
)

SELECT * FROM "aggregated_data";