-- PostgreSQL
-- 查询注册客户的基本信息及其对应经营岗数据
-- 1. 指定查询的时间范围
-- 2. 获取注册客户的基础信息
-- 3. 整合数据并按照注册日期和客户ID排序
WITH "date_range" AS (
    SELECT 
           {{start_date}}::DATE AS "start_date",
           {{end_date}}::DATE AS "end_date"
),
"registrated_store" AS (
    SELECT "ads"."store_profile"."tag_types" AS "tag_name",
           "ads"."store_profile"."store_id",
           "live"."stores"."name" AS "store_name",
           "ads"."store_profile"."salesman_id" AS "registratman_id",
           "ads"."store_profile"."first_order_serviceman_id",
           "ads"."store_profile"."serviceman_id",
           "ads"."store_profile"."created_at" AS "registrated_time",
           "ads"."store_profile"."created_at"::DATE AS "registrated_date",
           "ads"."store_profile"."first_order_settlement_time",
           "ads"."store_profile"."first_order_settlement_time"::DATE AS "first_order_settlement_date"
      FROM "ads"."store_profile"
     INNER JOIN "live"."stores"
             ON "ads"."store_profile"."store_id" = "live"."stores"."id"
     CROSS JOIN "date_range"
     WHERE "ads"."store_profile"."created_at" >= "date_range"."start_date"
       AND "ads"."store_profile"."created_at" <= "date_range"."end_date" + INTERVAL '1 DAYS'
),
"final_result" AS (
    SELECT "registrated_store"."tag_name" AS "门类",
           "registrated_store"."store_id" AS "客户ID",
           "registrated_store"."store_name" AS "客户名称",
           "registrated_store"."registratman_id" AS "注册经营岗",
           "registrated_store"."first_order_serviceman_id" AS "首单经营岗",
           "registrated_store"."serviceman_id" AS "当前经营岗",
           "registrated_store"."registrated_time" AS "注册时间",
           "registrated_store"."registrated_date" AS "注册日期",
           "registrated_store"."first_order_settlement_time" AS "首单结算时间",
           "registrated_store"."first_order_settlement_date" AS "首单结算日期"
      FROM "registrated_store"
     ORDER BY 8, 2
)
SELECT *
  FROM "final_result";