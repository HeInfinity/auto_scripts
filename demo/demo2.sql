-- PostgreSQL
-- 客户销售数据分析查询 - 稳定优化版本
-- 功能: 分析客户的销售数据, 包括客户分类, 产值统计, 划转分析等
-- 最后更新: 2025-06-30
-- 优化: 删除未使用CTE, 简化逻辑表达式, 优化可读性, 确保稳定运行

-- 时间变量定义
WITH "time_vars" AS (
    SELECT 
        '2025-06-01 00:00:00'::TIMESTAMP AS "start_date", 
        '2025-06-22 23:59:59'::TIMESTAMP AS "end_date"
),



-- 员工客户基础关系 (解决门类重复问题)
"employee_customer_base" AS (
    SELECT
        -- 组织架构信息
        "sub"."name" AS "subsidiary_name",
        "rg"."name" AS "region_name", 
        "dc"."id" AS "delivery_center_id",
        "dc"."name" AS "delivery_center_name",
        "e"."dc_id" AS "employee_dc_id",
        "e"."id" AS "employee_id",
        
        -- 员工信息 (聚合处理多角色)
        STRING_AGG(DISTINCT "e"."name", ',') AS "employee_name",
        STRING_AGG(DISTINCT "ro"."name", ',') AS "role_name",
        
        -- 客户基础信息
        "s"."id" AS "customer_id",
        "s"."name" AS "customer_name",
        "u"."phone" AS "customer_phone",
        "s"."level" AS "customer_level",
        
        -- 门类信息 (聚合处理多门类)
        STRING_AGG(DISTINCT "ty"."name", ',') AS "primary_category",
        STRING_AGG(DISTINCT "tg"."name", ',') AS "secondary_category",
        
        -- 时间信息
        "s"."activated_at" AS "registration_time",
        "s"."lock_time" AS "customer_loss_time",
        "s"."receive_at" AS "public_pool_receive_time"
    FROM "live"."employees" AS "e"
    INNER JOIN "live"."employeeroles" AS "er" ON "er"."employee_id" = "e"."id"
    INNER JOIN "live"."roles" AS "ro" ON "er"."role_id" = "ro"."id"
    INNER JOIN "live"."stores" AS "s" ON "s"."serviceman_id" = "e"."id"
    INNER JOIN "live"."users" AS "u" ON "u"."id" = "s"."owner_id"
    INNER JOIN "live"."delivercenters" AS "dc" ON "dc"."id" = "s"."dc_id"
    INNER JOIN "live"."regionals" AS "rg" ON "rg"."id" = "dc"."regional_id"
    INNER JOIN "live"."subsidiaries" AS "sub" ON "sub"."id" = "dc"."sub_id"
    LEFT JOIN "live"."storetags" AS "sg" ON "s"."id" = "sg"."store_id"
    LEFT JOIN "live"."tags" AS "tg" ON "sg"."tag_id" = "tg"."id"
    LEFT JOIN "live"."tagtypes" AS "ty" ON "tg"."type_id" = "ty"."id"
    WHERE "rg"."id" IN (1, 2, 5, 7, 8, 9, 10, 12)  -- 指定大区范围
        AND "dc"."id" NOT IN (196, 107, 203, 92)     -- 排除特定营业部
        AND ("ro"."id" = 3  -- 角色ID为3
            OR "e"."id" IN (
                5714, 2252, 5086, 6172, 6602, 4252, 4225, 7811, 7904, 6617,
                6476, 6623, 4050, 7771, 7770, 7989, 8411, 8617, 8634, 8638,
                1867, 8681, 8741, 8793, 9067, 9081, 9099, 9119, 9128, 9184,
                9158, 7432, 9197, 9193, 9228, 9233, 9241, 9266, 9308, 9289,
                9344, 9302, 9341, 9400, 9407, 9403, 3036
            ))  -- 指定员工ID列表
        AND "e"."status" = 'enabled'      -- 员工状态为启用
        AND "s"."status" = 'ACTIVATION'   -- 客户状态为激活
        AND "s"."flow_type" = 'ACTIVE'    -- 流程类型为活跃
        AND "s"."enable" = 1              -- 客户启用
        AND "s"."business_type" <> 'CLOSE' -- 业务类型非关闭
    GROUP BY "sub"."name", "rg"."name", "dc"."id", "dc"."name", "e"."dc_id", "e"."id", 
             "s"."id", "s"."name", "u"."phone", "s"."level", "s"."activated_at", 
             "s"."lock_time", "s"."receive_at"
),

-- 客户定价SKU统计
"pricing_stats" AS (
    SELECT
        "sp"."store_id" AS "customer_id",
        COUNT(DISTINCT "spi"."product_id") AS "priced_sku_count",
        COUNT(DISTINCT "ss"."product_id") AS "promoted_sku_count"
    FROM "live"."storeprices" AS "sp"
    INNER JOIN "live"."storepriceitems" AS "spi" ON "spi"."parent_id" = "sp"."id"
        AND "sp"."status" = 'START'  -- 定价状态为开始
    LEFT JOIN "dws"."sales_day_stats" AS "ss" ON "ss"."store_id" = "sp"."store_id"
        AND "ss"."product_id" = "spi"."product_id"
        AND "ss"."settlement_day" BETWEEN (SELECT "start_date" FROM "time_vars") 
        AND (SELECT "end_date" FROM "time_vars")  -- 指定时间范围
    GROUP BY "sp"."store_id"
),

-- 员工角色处理 (优先级: 37 > 79 > 其他)
"employee_roles" AS (
    SELECT
        "er"."employee_id",
        CASE
            WHEN 37 = ANY(ARRAY_AGG(DISTINCT "er"."role_id")) THEN 37
            WHEN 79 = ANY(ARRAY_AGG(DISTINCT "er"."role_id")) THEN 79
            ELSE (ARRAY_AGG(DISTINCT "er"."role_id" ORDER BY "er"."role_id" DESC)::INTEGER[])[1]
        END AS "primary_role_id"
    FROM "live"."employeeroles" AS "er"
    GROUP BY "er"."employee_id"
),

-- 客户分析主表 (包含客户类型分类)
"customer_analysis" AS (
    SELECT
        "ecb".*,
        
        -- 定价状态判断
        CASE WHEN "ps"."priced_sku_count" > 0 THEN '是' ELSE '否' END AS "has_pricing",
        COALESCE("ps"."priced_sku_count", 0) AS "priced_sku_count",
        COALESCE("ps"."promoted_sku_count", 0) AS "promoted_sku_count",
        
        -- 订单事件信息
        "ve"."month_before_serviceman_id" AS "prev_month_last_serviceman",
        "ve"."month_before_order_time" AS "prev_month_last_order_time",
        "ve"."month_first_serviceman_id" AS "curr_month_first_serviceman",
        "ve"."month_first_order_time" AS "curr_month_first_order_time",
        "ve"."month_first_dc_id" AS "curr_month_first_dc_id",
        "er1"."primary_role_id" AS "first_order_role_id",
        "ve"."interval_days" AS "days_between_orders",
        
        -- 流失天数计算
        CASE
            WHEN "ve"."month_first_order_time" IS NULL THEN
                ((SELECT "end_date" FROM "time_vars")::DATE - "ve"."month_before_order_time"::DATE)
        END AS "customer_lost_days",
        
                 -- 客户类型分类逻辑 (优化可读性)
        CASE
            WHEN "ve"."month_before_order_time" IS NULL 
                AND "ve"."month_first_order_time" IS NOT NULL THEN
                '首单客户' 
            WHEN "ve"."interval_days" BETWEEN 30 AND 89
                AND "ve"."month_first_serviceman_id" <> "ve"."month_before_serviceman_id" THEN
                '超30日未下单非本人客户拉回'
            WHEN "ve"."interval_days" >= 90 THEN
                '超90日外未下单客户拉回' 
            WHEN "ve"."month_first_order_time" IS NULL THEN
                CASE
                    WHEN ((SELECT "end_date" FROM "time_vars")::DATE - "ve"."month_before_order_time"::DATE) BETWEEN 30 AND 89 THEN
                        '超30日流失未下单客户'
                    WHEN ((SELECT "end_date" FROM "time_vars")::DATE - "ve"."month_before_order_time"::DATE) >= 90 THEN
                        '超90日流失未下单客户'
                    ELSE NULL
                END
            WHEN "ve"."month_before_order_time" IS NULL 
                AND "ve"."month_first_order_time" IS NULL THEN
                '未下单客户'
            ELSE NULL
        END AS "customer_type",
        
        -- 末单信息
        "ve"."month_last_serviceman_id" AS "curr_month_last_serviceman",
        "ve"."month_last_dc_id" AS "curr_month_last_dc_id",
        "er2"."primary_role_id" AS "last_order_role_id",
        "ve"."month_last_order_time" AS "curr_month_last_order_time",
        "ve"."first_order_time" AS "first_ever_order_time",
        COALESCE("sv"."visit_num", 0) AS "total_visit_count"
    FROM "employee_customer_base" AS "ecb"
    LEFT JOIN "pricing_stats" AS "ps" ON "ps"."customer_id" = "ecb"."customer_id"
    LEFT JOIN "dwd"."v_store_order_events" AS "ve" ON "ve"."store_id" = "ecb"."customer_id"
    LEFT JOIN "dws"."store_visits_month_stats" AS "sv" ON "sv"."serviceman_id" = "ecb"."employee_id"
        AND "sv"."data_id" = "ecb"."customer_id"
        AND "sv"."visit_month" = (SELECT "start_date" FROM "time_vars")
    LEFT JOIN "employee_roles" AS "er1" ON "er1"."employee_id" = "ve"."month_first_serviceman_id"
    LEFT JOIN "employee_roles" AS "er2" ON "er2"."employee_id" = "ve"."month_last_serviceman_id"
),

-- 销售数据聚合分析
"sales_summary" AS (
    SELECT
        "ca"."subsidiary_name",
        "ca"."region_name",
        "ca"."delivery_center_id",
        "ca"."delivery_center_name",
        "ca"."employee_dc_id",
        "ca"."employee_id",
        "ca"."employee_name",
        "ca"."role_name",
        "ca"."customer_id",
        "ca"."customer_name",
        "ca"."customer_phone",
        "ca"."customer_level",
        "ca"."primary_category",
        "ca"."secondary_category",
        "ca"."registration_time",
        "ca"."customer_loss_time",
        "ca"."public_pool_receive_time",
        "ca"."has_pricing",
        "ca"."priced_sku_count",
        "ca"."promoted_sku_count",
        "ca"."prev_month_last_serviceman",
        "ca"."prev_month_last_order_time",
        "ca"."curr_month_first_serviceman",
        "ca"."curr_month_first_order_time",
        "ca"."curr_month_first_dc_id",
        "ca"."first_order_role_id",
        "ca"."days_between_orders",
        "ca"."customer_lost_days",
        "ca"."customer_type",
        "ca"."curr_month_last_serviceman",
        "ca"."curr_month_last_dc_id",
        "ca"."last_order_role_id",
        "ca"."curr_month_last_order_time",
        "ca"."first_ever_order_time",
        "ca"."total_visit_count",
        
        -- 营业部数量统计
        COUNT("ss"."dc_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) AS "active_dc_count",
        
        -- 划转类型判断
        CASE
            WHEN COUNT(DISTINCT "ss"."dc_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) = 2
                AND COUNT(DISTINCT "ss"."serviceman_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) = 2 THEN
                '营业部划转' 
            WHEN COUNT(DISTINCT "ss"."dc_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 2
                AND COUNT(DISTINCT "ss"."serviceman_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 3 THEN
                '营业部划转-经营岗超3个' 
            WHEN COUNT(DISTINCT "ss"."dc_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) = 1
                AND COUNT(DISTINCT "ss"."serviceman_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 2 THEN
                '营业部不变，经营岗划转' 
        END AS "transfer_type",
        
                 -- 高粘客户判断 (优化子公司判断逻辑)
        CASE "ca"."subsidiary_name"
            WHEN '成都子公司' THEN
                CASE WHEN SUM("ss"."sales_value") >= 5000
                    AND COUNT(DISTINCT "ss"."settlement_day") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 8
                    AND COUNT(DISTINCT "ss"."first_cate_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 5 
                    THEN '是' END
            WHEN '重庆子公司' THEN
                CASE WHEN SUM("ss"."sales_value") >= 4000
                    AND COUNT(DISTINCT "ss"."settlement_day") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 8
                    AND COUNT(DISTINCT "ss"."first_cate_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 5 
                    THEN '是' END
            WHEN '南京子公司' THEN
                CASE WHEN SUM("ss"."sales_value") >= 3000
                    AND COUNT(DISTINCT "ss"."settlement_day") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 8
                    AND COUNT(DISTINCT "ss"."first_cate_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) >= 5 
                    THEN '是' END
        END AS "is_high_sticky_customer",
        
        -- 当前经营岗产值和毛利
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ca"."employee_id" = "ss"."serviceman_id"), 0) AS "current_serviceman_sales",
        COALESCE(SUM("ss"."profit") FILTER (WHERE "ca"."employee_id" = "ss"."serviceman_id"), 0) AS "current_serviceman_profit",
        
        -- 下单统计指标
        COUNT(DISTINCT "ss"."settlement_day") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) AS "active_order_days",
        COUNT(DISTINCT "ss"."first_cate_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) AS "total_categories",
        COUNT(DISTINCT "ss"."product_id") FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) AS "total_skus",
        
        -- 品类订单数统计 (使用ROW构造替代CONCAT)
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0) AS "total_category_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1001) AS "vegetable_fruit_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1009) AS "fresh_meat_egg_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1002) AS "frozen_food_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1004) AS "grain_oil_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1005) AS "seasoning_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1006) AS "dry_goods_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1007) AS "beverage_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1008) AS "kitchen_goods_orders",
        COUNT(DISTINCT ("ss"."first_cate_id", "ss"."store_id", "ss"."settlement_day")) 
            FILTER (WHERE "ss"."sales_value" - "ss"."return_value" > 0 AND "ss"."first_cate_id" = 1992) AS "seafood_orders",
        
        -- 总产值和各品类产值
        COALESCE(SUM("ss"."sales_value"), 0) AS "total_sales_value",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1001), 0) AS "vegetable_fruit_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1009), 0) AS "fresh_meat_egg_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1002), 0) AS "frozen_food_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1004), 0) AS "grain_oil_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1005), 0) AS "seasoning_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1006), 0) AS "dry_goods_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1007), 0) AS "beverage_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1008), 0) AS "kitchen_goods_sales",
        COALESCE(SUM("ss"."sales_value") FILTER (WHERE "ss"."first_cate_id" = 1992), 0) AS "seafood_sales"
    FROM "customer_analysis" AS "ca"
    LEFT JOIN "dws"."sales_day_stats" AS "ss" ON "ss"."store_id" = "ca"."customer_id"
        AND "ss"."settlement_day" BETWEEN (SELECT "start_date" FROM "time_vars") 
        AND (SELECT "end_date" FROM "time_vars")
    WHERE "ca"."customer_type" IS NOT NULL 
        AND (
            ("ca"."delivery_center_id" = "ca"."employee_dc_id" AND "ca"."role_name" LIKE '%营业部经理%')
            OR ("ca"."role_name" LIKE '%KA经理-自营%')
            OR ("ca"."role_name" LIKE '%KA专员-自营%')
            OR ("ca"."delivery_center_id" = "ca"."employee_dc_id" AND "ca"."role_name" LIKE '%营业部主管%')
        )
    GROUP BY "ca"."subsidiary_name", "ca"."region_name", "ca"."delivery_center_id", 
             "ca"."delivery_center_name", "ca"."employee_dc_id", "ca"."employee_id", 
             "ca"."employee_name", "ca"."role_name", "ca"."customer_id", "ca"."customer_name", 
             "ca"."customer_phone", "ca"."customer_level", "ca"."primary_category", 
             "ca"."secondary_category", "ca"."registration_time", "ca"."customer_loss_time", 
             "ca"."public_pool_receive_time", "ca"."has_pricing", "ca"."priced_sku_count", 
             "ca"."promoted_sku_count", "ca"."prev_month_last_serviceman", 
             "ca"."prev_month_last_order_time", "ca"."curr_month_first_serviceman", 
             "ca"."curr_month_first_order_time", "ca"."curr_month_first_dc_id", 
             "ca"."first_order_role_id", "ca"."days_between_orders", "ca"."customer_lost_days", 
             "ca"."customer_type", "ca"."curr_month_last_serviceman", "ca"."curr_month_last_dc_id", 
             "ca"."last_order_role_id", "ca"."curr_month_last_order_time", 
             "ca"."first_ever_order_time", "ca"."total_visit_count"
),

-- 最终结果输出
"final_report" AS (
    SELECT
        "ss"."subsidiary_name" AS "子公司",
        "ss"."region_name" AS "大区",
        "ss"."delivery_center_name" AS "营业部",
        "ss"."employee_id" AS "员工编码",
        "ss"."employee_name" AS "经营人员",
        "ss"."role_name" AS "岗位",
        "ss"."customer_type" AS "客户类型",
        "ss"."customer_id" AS "客户编码",
        "ss"."customer_name" AS "客户名称",
        "ss"."customer_level" AS "客户等级",
        "ss"."primary_category" AS "一级门类",
        "ss"."secondary_category" AS "二级门类",
        "ss"."has_pricing" AS "是否定价",
        "ss"."priced_sku_count" AS "定价SKU数",
        "ss"."promoted_sku_count" AS "推品sku数",
        "ss"."registration_time" AS "注册时间",
        "ss"."first_ever_order_time" AS "首单结算时间",
        "ss"."curr_month_last_order_time" AS "末单时间",
        "ss"."public_pool_receive_time" AS "公海领取时间",
        
        -- 产值和毛利 (使用NULLIF优化0值处理)
        NULLIF("ss"."current_serviceman_sales", 0) AS "现经营岗_净新增产值",
        NULLIF("ss"."current_serviceman_profit", 0) AS "现经营岗_净新增毛利",
        
        -- 毛利率计算
        CASE
            WHEN "ss"."current_serviceman_sales" > 0 AND "ss"."current_serviceman_profit" > 0 THEN
                ROUND(("ss"."current_serviceman_profit" / "ss"."current_serviceman_sales") * 100, 2) || '%'
        END AS "现经营岗_净新增毛利率",
        
        "ss"."total_visit_count" AS "累计拜访次数",
        "ss"."is_high_sticky_customer" AS "是否高粘客户",
        NULLIF("ss"."total_sales_value", 0) AS "总产值",
        "ss"."active_order_days" AS "本月下单天数",
        "ss"."total_categories" AS "累计品类数",
        "ss"."total_skus" AS "累计SKU数",
        "ss"."total_category_orders" AS "累计品类订单数",
        
        -- 分品类订单数 (使用NULLIF优化0值处理)
        NULLIF("ss"."vegetable_fruit_orders", 0) AS "蔬菜水果_订单数",
        NULLIF("ss"."fresh_meat_egg_orders", 0) AS "鲜肉蛋_订单数",
        NULLIF("ss"."frozen_food_orders", 0) AS "冻品_订单数",
        NULLIF("ss"."grain_oil_orders", 0) AS "米面粮油_订单数",
        NULLIF("ss"."seasoning_orders", 0) AS "调味品_订单数",
        NULLIF("ss"."dry_goods_orders", 0) AS "干杂_订单数",
        NULLIF("ss"."beverage_orders", 0) AS "酒水饮料_订单数",
        NULLIF("ss"."kitchen_goods_orders", 0) AS "餐厨百货_订单数",
        NULLIF("ss"."seafood_orders", 0) AS "海鲜水产_订单数",
        
        -- 分品类产值 (使用NULLIF优化0值处理)
        NULLIF("ss"."vegetable_fruit_sales", 0) AS "蔬菜水果_产值",
        NULLIF("ss"."fresh_meat_egg_sales", 0) AS "鲜肉蛋_产值",
        NULLIF("ss"."frozen_food_sales", 0) AS "冻品_产值",
        NULLIF("ss"."grain_oil_sales", 0) AS "米面粮油_产值",
        NULLIF("ss"."seasoning_sales", 0) AS "调味品_产值",
        NULLIF("ss"."dry_goods_sales", 0) AS "干杂_产值",
        NULLIF("ss"."beverage_sales", 0) AS "酒水饮料_产值",
        NULLIF("ss"."kitchen_goods_sales", 0) AS "餐厨百货_产值",
        NULLIF("ss"."seafood_sales", 0) AS "海鲜水产_产值"
    FROM "sales_summary" AS "ss"
)

SELECT * FROM "final_report";