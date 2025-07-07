-- PostgreSQL
-- 品类销售数据聚合分析

WITH "date_range" AS (
     SELECT '2025-06-01'::DATE AS "start_date",
			'2025-06-16'::DATE AS "end_date"
),

"chinese_final_result" AS (
	SELECT "live"."dwd"."delivercenter_info"."sub_name" AS "分子公司",
				 DATE_TRUNC('month', "live"."dws"."sales_day_stats"."settlement_day")::DATE AS "结算月份",
				 "live"."dwd"."category_info"."first_name" AS "一级品类",
				 "live"."dwd"."category_info"."second_name" AS "二级品类",
				 "live"."dwd"."category_info"."name" AS "三级品类",
				 "live"."dws"."sales_day_stats"."spu_id" AS "三级品类ID",
				 "live"."dws"."sales_day_stats"."vendor_id" AS  "供应商ID",
				 "live"."live"."vendors"."name" AS "供应商名称",
				 SUM("live"."dws"."sales_day_stats"."sales_value") AS "结算净产值",
				 SUM("live"."dws"."sales_day_stats"."profit") AS "结算净毛利",
				 SUM("live"."dws"."sales_day_stats"."sales_quantity") AS "结算净销量",
				 COUNT(DISTINCT "live"."dws"."sales_day_stats"."store_id") AS "下单客户数",
				 CASE 
					WHEN SUM("live"."dws"."sales_day_stats"."sales_quantity") = 0 THEN 0
					ELSE (SUM("live"."dws"."sales_day_stats"."sales_value") - SUM("live"."dws"."sales_day_stats"."profit")) / SUM("live"."dws"."sales_day_stats"."sales_quantity")
				 END AS "成本单价"
				 
		FROM "live"."dwd"."category_info"
	 INNER JOIN "live"."dws"."sales_day_stats"
				 ON "live"."dwd"."category_info"."id" = "live"."dws"."sales_day_stats"."spu_id"
	 INNER JOIN "live"."live"."vendors"
				 ON "live"."dws"."sales_day_stats"."vendor_id" = "live"."live"."vendors"."id"
	 INNER JOIN "live"."dwd"."delivercenter_info"
				 ON "live"."dws"."sales_day_stats"."dc_id" = "live"."dwd"."delivercenter_info"."id"
   CROSS JOIN "date_range"
	 WHERE "live"."dws"."sales_day_stats"."regional_id" IN (1, 2, 5, 8)
		 AND "live"."dws"."sales_day_stats"."settlement_day" >= "date_range"."start_date"
		 AND "live"."dws"."sales_day_stats"."settlement_day" <= "date_range"."end_date"
	 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT *
  FROM "chinese_final_result";