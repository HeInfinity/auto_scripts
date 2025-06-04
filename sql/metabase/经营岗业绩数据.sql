-- PostgreSQL
-- 获取当前月份的起止时间
WITH "date_range" AS (
		/*
     SELECT {{start_date}}::DATE AS "start_date",
						{{end_date}}::DATE AS "end_date"
	   */
     SELECT DATE_TRUNC('month', CURRENT_DATE)::DATE AS "start_date",
            (CURRENT_DATE - INTERVAL '1 day')::DATE AS "end_date"
),
-- 汇总每个客户的产值和毛利额
"sales_performance" AS (
     SELECT "dws"."sales_day_stats"."sub_id",
            "dws"."sales_day_stats"."regional_id",
            "dws"."sales_day_stats"."dc_id",
            "dws"."sales_day_stats"."store_id",
            "dws"."sales_day_stats"."serviceman_id",
            SUM("dws"."sales_day_stats"."sales_value") AS "sales_amount",
            SUM("dws"."sales_day_stats"."profit") AS "profit_amount",
            "ads"."store_profile"."first_order_settlement_time",
            "date_range"."start_date"
       FROM "dws"."sales_day_stats"
 INNER JOIN "ads"."store_profile"
         ON "dws"."sales_day_stats"."store_id" = "ads"."store_profile"."store_id"
 CROSS JOIN "date_range"
      WHERE "dws"."sales_day_stats"."settlement_day" BETWEEN "date_range"."start_date" AND "date_range"."end_date"
   GROUP BY 1, 2, 3, 4, 5, 8, 9
),
-- 关联基础信息获取客户和员工的信息
"sales_base" AS (
     SELECT "dwd"."delivercenter_info"."sub_name",
            "dwd"."delivercenter_info"."regional_name",
            "dwd"."delivercenter_info"."name" AS "dc_name",
            "sales_performance"."store_id",
            "live"."stores"."name" AS "store_name",
            "live"."employees"."name" AS "employee_name",
            "sales_performance"."serviceman_id",
            "sales_performance"."sales_amount",
            "sales_performance"."profit_amount",
            "sales_performance"."first_order_settlement_time",
            DATE_TRUNC('MONTH', "sales_performance"."first_order_settlement_time")::DATE AS "settlement_month",
            DATE_TRUNC('MONTH', "sales_performance"."start_date")::DATE AS "current_month"
       FROM "sales_performance"
 INNER JOIN "dwd"."delivercenter_info"
         ON "sales_performance"."dc_id" = "dwd"."delivercenter_info"."id"
 INNER JOIN "live"."stores"
         ON "sales_performance"."store_id" = "live"."stores"."id"
  LEFT JOIN "live"."employees"
         ON "sales_performance"."serviceman_id" = "live"."employees"."id"
),
-- 计算客户属性并修正营业部名称
"customer_attributes" AS (
     SELECT "sales_base".*,
            CASE WHEN "sales_base"."settlement_month" = "sales_base"."current_month" THEN '新客'
                 ELSE '老客' END AS "nextmonth_holding_capacity",
            CASE WHEN "sales_base"."settlement_month" < DATE_TRUNC('YEAR', "sales_base"."current_month")::DATE THEN '老客保有'
                 WHEN "sales_base"."settlement_month" = "sales_base"."current_month" THEN '当月新开'
                 ELSE '新客保有' END AS "comprehensive_holding_capacity",
            CASE WHEN "sales_base"."settlement_month" = "sales_base"."current_month" THEN '新客'
                 WHEN "sales_base"."first_order_settlement_time" >= ("sales_base"."current_month" - INTERVAL '1' MONTH) 
                      AND EXTRACT(DAY FROM "sales_base"."first_order_settlement_time") >= 25 THEN '新客'
                 ELSE '老客' END AS "after25_holding_capacity",
            CASE WHEN "sales_base"."dc_name" = '琉璃场营业部' THEN '海椒市横街营业部'
                 WHEN "sales_base"."dc_name" = '乐山高新区营业部' THEN '乐山市中区营业部'
                 WHEN "sales_base"."dc_name" = '青城山营业部' THEN '都江堰安顺营业部'
                 ELSE "sales_base"."dc_name" END AS "modify_dc_name"
       FROM "sales_base"
),
-- 为了便于审核每个CTE的查询结果, 使用SELECT *
"sales_report" AS (
     SELECT "customer_attributes"."sub_name" AS "分子公司",
            "customer_attributes"."regional_name" AS "大区",
            "customer_attributes"."dc_name" AS "营业部",
            "customer_attributes"."store_id" AS "客户ID",
            "customer_attributes"."store_name" AS "客户名称",
            "customer_attributes"."employee_name" AS "经营岗",
            "customer_attributes"."serviceman_id" AS "经营岗ID",
            "customer_attributes"."sales_amount" AS "产值",
            "customer_attributes"."profit_amount" AS "毛利额",
            "customer_attributes"."first_order_settlement_time" AS "首单结算时间",
            "customer_attributes"."settlement_month" AS "首单结算月",
            "customer_attributes"."current_month" AS "当月月份",
            "customer_attributes"."nextmonth_holding_capacity" AS "次月新老客",
            "customer_attributes"."comprehensive_holding_capacity" AS "去年新老客",
            "customer_attributes"."after25_holding_capacity" AS "25日后新老客",
            "customer_attributes"."modify_dc_name" AS "修正后营业部"
       FROM "customer_attributes"
   ORDER BY 1, 2, 3, 7, 4
)
SELECT *
  FROM "sales_report";