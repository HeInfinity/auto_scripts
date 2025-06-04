WITH "date_range" AS (
    SELECT 
      -- {{start_date}}::TIMESTAMP AS "start_date"
			'2025-01-01 00:00:00'::TIMESTAMP AS "start_date"
),
"sub_name" AS (
		SELECT
      -- {{sub_name}} AS "sub_name"
			'成都子公司' AS "sub_name"
),
"store_first_month" AS (
  SELECT
    "store_id",
    DATE_TRUNC('MONTH', "first_order_settlement_time")::DATE AS "month_first_day",
    "first_order_serviceman_id",
		"serviceman_id"
  FROM "ads"."store_profile"
 CROSS JOIN "date_range"
 WHERE "first_order_settlement_time" BETWEEN DATE_TRUNC('MONTH', "date_range"."start_date") AND (DATE_TRUNC('MONTH', "date_range"."start_date") + INTERVAL '1 MONTH')
   AND "first_order_serviceman_id" > 0
),
"month_seq" AS (
  SELECT
    "store_first_month"."store_id",
    "store_first_month"."month_first_day",
    "store_first_month"."first_order_serviceman_id",
    generate_series(0, 5) AS "month_offset",
    ("store_first_month"."month_first_day" + (INTERVAL '1 month' * generate_series(0, 5)))::DATE AS "month_date"
  FROM "store_first_month"
),
"sales_all_stores" AS (
  SELECT
    "store_id",
    DATE_TRUNC('MONTH', "settlement_day")::DATE AS "settlement_month",
    SUM("sales_value") AS "sales"
  FROM "dws"."sales_day_stats"
	CROSS JOIN "date_range"
  WHERE "settlement_day" BETWEEN DATE_TRUNC('MONTH', "date_range"."start_date")::DATE AND (DATE_TRUNC('MONTH', "date_range"."start_date") + INTERVAL '6 MONTH' - INTERVAL '1 DAY')::DATE
  GROUP BY 1, 2
),
"sales_pivot" AS (
  SELECT
    "month_seq"."store_id",
    "month_seq"."month_first_day",
    "month_seq"."first_order_serviceman_id",
    "month_seq"."month_offset" + 1 AS "month_num",
    CASE
      WHEN "month_seq"."month_date" > DATE_TRUNC('MONTH', CURRENT_DATE) THEN '-'
      ELSE COALESCE("sales_all_stores"."sales", 0)::text
    END AS "sales"
  FROM "month_seq"
  LEFT JOIN "sales_all_stores"
    ON "month_seq"."store_id" = "sales_all_stores"."store_id" AND "month_seq"."month_date" = "sales_all_stores"."settlement_month"
),
"forth_month_sales_raw" AS (
  SELECT 
    "dws"."sales_day_stats"."store_id" AS "store_id",
    "dws"."sales_day_stats"."settlement_day" AS "settlement_day",
    "dws"."sales_day_stats"."serviceman_id" AS "serviceman_id"
   FROM "dws"."sales_day_stats"
  INNER JOIN "store_first_month"
     ON "dws"."sales_day_stats"."store_id" = "store_first_month"."store_id"
  WHERE "dws"."sales_day_stats"."settlement_day" >= "store_first_month"."month_first_day" + INTERVAL '3 MONTH'
    AND "dws"."sales_day_stats"."settlement_day" <  "store_first_month"."month_first_day" + INTERVAL '4 MONTH'
),
"forth_month_sales_ranked" AS (
  SELECT 
    "store_id",
    "settlement_day",
    "serviceman_id",
    ROW_NUMBER() OVER (
      PARTITION BY "store_id" 
      ORDER BY "settlement_day" DESC
    ) AS "rn"
   FROM "forth_month_sales_raw"
),
"forth_month_serviceman_cte" AS (
  SELECT 
    "store_id" AS "store_id",
    "serviceman_id" AS "forth_month_serviceman_id"
   FROM "forth_month_sales_ranked"
  WHERE "rn" = 1
),
"month_sales_raw" AS (
  SELECT 
    "dws"."sales_day_stats"."store_id" AS "store_id",
    "dws"."sales_day_stats"."settlement_day" AS "settlement_day",
    "dws"."sales_day_stats"."serviceman_id" AS "serviceman_id"
   FROM "dws"."sales_day_stats"
  INNER JOIN "store_first_month"
     ON "dws"."sales_day_stats"."store_id" = "store_first_month"."store_id"
  WHERE "dws"."sales_day_stats"."settlement_day" <  "store_first_month"."month_first_day" + INTERVAL '4 MONTH'
    AND "dws"."sales_day_stats"."serviceman_id" > 0
),
"month_sales_ranked" AS (
  SELECT 
    "store_id",
    "settlement_day",
    "serviceman_id",
    ROW_NUMBER() OVER (
      PARTITION BY "store_id" 
      ORDER BY "settlement_day" DESC
    ) AS "rn"
   FROM "month_sales_raw"
),
"month_serviceman_cte" AS (
  SELECT 
    "store_id" AS "store_id",
    "serviceman_id" AS "month_serviceman_id"
   FROM "month_sales_ranked"
  WHERE "rn" = 1
),
"Result" AS (
	SELECT 
		"store_first_month"."store_id" AS "store_id",
		"sales_pivot"."month_first_day",
		"store_first_month"."first_order_serviceman_id" AS "first_order_serviceman_id",
		COALESCE("forth_month_serviceman_cte"."forth_month_serviceman_id", "store_first_month"."serviceman_id") AS "forth_month_serviceman_id",
		MAX(CASE WHEN "sales_pivot"."month_num" = 1 THEN "sales_pivot"."sales" END) AS "first_month_sales",
		MAX(CASE WHEN "sales_pivot"."month_num" = 2 THEN "sales_pivot"."sales" END) AS "second_month_sales",
		MAX(CASE WHEN "sales_pivot"."month_num" = 3 THEN "sales_pivot"."sales" END) AS "third_month_sales",
		MAX(CASE WHEN "sales_pivot"."month_num" = 4 THEN "sales_pivot"."sales" END) AS "fourth_month_sales",
		MAX(CASE WHEN "sales_pivot"."month_num" = 5 THEN "sales_pivot"."sales" END) AS "fifth_month_sales",
		MAX(CASE WHEN "sales_pivot"."month_num" = 6 THEN "sales_pivot"."sales" END) AS "sixth_month_sales"
	FROM "store_first_month"
	LEFT JOIN "forth_month_serviceman_cte"
		ON "store_first_month"."store_id" = "forth_month_serviceman_cte"."store_id"
	LEFT JOIN "sales_pivot"
		ON "store_first_month"."store_id" = "sales_pivot"."store_id" AND "store_first_month"."month_first_day" = "sales_pivot"."month_first_day"
	GROUP BY "store_first_month"."store_id", "sales_pivot"."month_first_day", "store_first_month"."first_order_serviceman_id", COALESCE("forth_month_serviceman_cte"."forth_month_serviceman_id", "store_first_month"."serviceman_id")
	ORDER BY 2, 1
),
"Chinese_Result" AS (
	SELECT 
				 "dwd"."delivercenter_info"."sub_name" AS "子公司",
				 "dwd"."delivercenter_info"."regional_name" AS "大区",
				 "dwd"."delivercenter_info"."name" AS "营业部",
				 "Result"."store_id" AS "客户编号",
				 "live"."stores"."name" AS "客户名称",
				 "Result"."month_first_day" AS "首单月",
				 "Result"."first_order_serviceman_id" AS "首单经营岗",
				 CASE WHEN "Result"."forth_month_serviceman_id" = 0 THEN "month_serviceman_cte"."month_serviceman_id" ELSE "Result"."forth_month_serviceman_id" END AS "第四月经营岗",
				 "Result"."first_month_sales" AS "第一月产值",
				 "Result"."second_month_sales" AS "第二月产值",
				 "Result"."third_month_sales" AS "第三月产值",
				 "Result"."fourth_month_sales" AS "第四月产值",
				 "Result"."fifth_month_sales" AS "第五月产值",
				 "Result"."sixth_month_sales" AS "第六月产值"
		FROM "Result"
	 INNER JOIN "live"."stores"
				 ON "Result"."store_id" = "live"."stores"."id"
	 INNER JOIN "dwd"."delivercenter_info"
				 ON "live"."stores"."dc_id" = "dwd"."delivercenter_info"."id"
	 INNER JOIN "month_serviceman_cte"
				 ON "Result"."store_id" = "month_serviceman_cte"."store_id"
	 -- WHERE "dwd"."delivercenter_info"."sub_name" IN (SELECT "sub_name"."sub_name" FROM "sub_name")
	 ORDER BY 1, 2, 3, 8, 4
 )
 SELECT *
   FROM "Chinese_Result";