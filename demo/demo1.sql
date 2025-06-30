-- MySQL
-- 员工业绩分析SQL - 高性能优化版本
-- 优化策略: 1. 避免在查询列上使用函数以保证索引生效 2. 优化聚合逻辑

-- =============================================================================
-- 目标员工常量 - 避免重复IN条件解析
-- =============================================================================
WITH `target_employees` AS (
    SELECT `employee_id`
    FROM (
        SELECT 5714 AS `employee_id` UNION ALL SELECT 2252 UNION ALL SELECT 5086 UNION ALL SELECT 6172 UNION ALL 
        SELECT 6602 UNION ALL SELECT 4252 UNION ALL SELECT 4225 UNION ALL SELECT 7811 UNION ALL SELECT 7904 UNION ALL 
        SELECT 6617 UNION ALL SELECT 6476 UNION ALL SELECT 6623 UNION ALL SELECT 4050 UNION ALL SELECT 7771 UNION ALL 
        SELECT 7770 UNION ALL SELECT 7989 UNION ALL SELECT 8411 UNION ALL SELECT 8617 UNION ALL SELECT 8634 UNION ALL 
        SELECT 8638 UNION ALL SELECT 1867 UNION ALL SELECT 8681 UNION ALL SELECT 8741 UNION ALL SELECT 8793 UNION ALL 
        SELECT 9067 UNION ALL SELECT 9081 UNION ALL SELECT 9099 UNION ALL SELECT 9119 UNION ALL SELECT 9128 UNION ALL 
        SELECT 9184 UNION ALL SELECT 9158 UNION ALL SELECT 7432 UNION ALL SELECT 9197 UNION ALL SELECT 9193 UNION ALL 
        SELECT 9228 UNION ALL SELECT 9233 UNION ALL SELECT 9241 UNION ALL SELECT 9266 UNION ALL SELECT 9308 UNION ALL 
        SELECT 9289 UNION ALL SELECT 9344 UNION ALL SELECT 9302 UNION ALL SELECT 9341 UNION ALL SELECT 9400 UNION ALL 
        SELECT 9407 UNION ALL SELECT 9403
    ) AS `emp_list`
),

-- =============================================================================
-- 日期边界计算 - 使用TIMESTAMP以利用索引
-- =============================================================================
`date_ranges` AS (
    SELECT 
        TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 15 DAY)) AS `last_week_start`,
        TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 7 DAY)) AS `this_week_start`,
        TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY)) AS `this_week_end`, -- 使用开区间 <
        TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) AS `yesterday_start`,
        TIMESTAMP(CURDATE()) AS `yesterday_end`, -- 使用开区间 <
        TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL 35 DAY)) AS `data_start_limit`
),

-- =============================================================================
-- 员工基础信息 - 仅查询目标员工，减少后续JOIN开销
-- =============================================================================
`employee_info` AS (
    SELECT
        `e`.`id` AS `employee_id`,
        `ss`.`NAME` AS `subsidiary_name`,
        `e`.`NAME` AS `employee_name`
    FROM
        `employees` AS `e`
        INNER JOIN `target_employees` AS `te` ON `te`.`employee_id` = `e`.`id`
        LEFT JOIN `subsidiaries` AS `ss` ON `e`.`sub_id` = `ss`.`id`
),

-- =============================================================================
-- 统一业绩数据源 - 先过滤再合并，大幅减少数据量
-- =============================================================================
`performance_base_data` AS (
    -- 订单数据
    SELECT
        `o`.`serviceman_id`,
        `o`.`settlement_time`,
        `oi`.`real_unit_price` * `oi`.`real_ud_quantity` AS `value`,
        (`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity` AS `gross_profit`
    FROM
        `orders` AS `o`
        INNER JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
        INNER JOIN `target_employees` AS `te` ON `te`.`employee_id` = `o`.`serviceman_id`
        CROSS JOIN `date_ranges` AS `dr`
    WHERE
        `o`.`settlement_time` >= `dr`.`data_start_limit`
        AND `o`.`settlement_time` < `dr`.`this_week_end`

    UNION ALL

    -- 退货数据 (负值)
    SELECT
        `o`.`serviceman_id`,
        `ret`.`settlement_time`,
        - (`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `value`,
        - (`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `gross_profit`
    FROM
        `OrderReturns` AS `ret`
        INNER JOIN `orders` AS `o` ON `o`.`id` = `ret`.`order_id`
        INNER JOIN `target_employees` AS `te` ON `te`.`employee_id` = `o`.`serviceman_id`
        CROSS JOIN `date_ranges` AS `dr`
    WHERE
        `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
        AND `ret`.`settlement_time` >= `dr`.`data_start_limit`
        AND `ret`.`settlement_time` < `dr`.`this_week_end`
),

-- =============================================================================
-- 聚合业绩数据 - 从预处理的数据源进行计算, 避免使用DATE()函数
-- =============================================================================
`unified_performance` AS (
    SELECT
        `pbd`.`serviceman_id` AS `employee_id`,
        
        -- 上周业绩指标
        SUM(CASE 
            WHEN `pbd`.`settlement_time` >= `dr`.`last_week_start` 
                AND `pbd`.`settlement_time` < `dr`.`this_week_start`
            THEN `pbd`.`value` ELSE 0 
        END) AS `last_week_value`,
        SUM(CASE 
            WHEN `pbd`.`settlement_time` >= `dr`.`last_week_start` 
                AND `pbd`.`settlement_time` < `dr`.`this_week_start`
            THEN `pbd`.`gross_profit` ELSE 0 
        END) AS `last_week_gross_profit`,
        
        -- 本周业绩指标
        SUM(CASE 
            WHEN `pbd`.`settlement_time` >= `dr`.`this_week_start` 
                AND `pbd`.`settlement_time` < `dr`.`this_week_end`
            THEN `pbd`.`value` ELSE 0 
        END) AS `this_week_value`,
        SUM(CASE 
            WHEN `pbd`.`settlement_time` >= `dr`.`this_week_start` 
                AND `pbd`.`settlement_time` < `dr`.`this_week_end`
            THEN `pbd`.`gross_profit` ELSE 0 
        END) AS `this_week_gross_profit`,
        
        -- 昨日业绩指标
        SUM(CASE 
            WHEN `pbd`.`settlement_time` >= `dr`.`yesterday_start`
                AND `pbd`.`settlement_time` < `dr`.`yesterday_end`
            THEN `pbd`.`value` ELSE 0 
        END) AS `yesterday_value`
    FROM
        `performance_base_data` AS `pbd`
        CROSS JOIN `date_ranges` AS `dr`
    GROUP BY `pbd`.`serviceman_id`
),

-- =============================================================================
-- 拜访数据优化 - 先UNION ALL再聚合
-- =============================================================================
`visit_stats` AS (
    SELECT
        `all_visits`.`employee_id`,
        SUM(CASE WHEN `all_visits`.`visit_time` >= `dr`.`last_week_start` AND `all_visits`.`visit_time` < `dr`.`this_week_start` THEN 1 ELSE 0 END) AS `last_week_visit_count`,
        SUM(CASE WHEN `all_visits`.`visit_time` >= `dr`.`this_week_start` AND `all_visits`.`visit_time` < `dr`.`this_week_end` THEN 1 ELSE 0 END) AS `this_week_visit_count`, 
        SUM(CASE WHEN `all_visits`.`visit_time` >= `dr`.`yesterday_start` AND `all_visits`.`visit_time` < `dr`.`yesterday_end` THEN 1 ELSE 0 END) AS `yesterday_visit_count`
    FROM (
        -- 拜访记录
        SELECT 
            `vr`.`visitor` AS `employee_id`,
            `vr`.`visit_time`
        FROM 
            `visitrecorditems` AS `vr`
            INNER JOIN `target_employees` AS `te` ON `te`.`employee_id` = `vr`.`visitor`
            CROSS JOIN `date_ranges` AS `dr`
        WHERE `vr`.`visit_time` >= `dr`.`data_start_limit` AND `vr`.`visit_time` < `dr`.`this_week_end`
        
        UNION ALL
        
        -- 大客户跟进记录
        SELECT 
            `lf`.`principal_id` AS `employee_id`,
            `lf`.`link_time` AS `visit_time`
        FROM 
            `largecsfollowups` AS `lf`
            INNER JOIN `target_employees` AS `te` ON `te`.`employee_id` = `lf`.`principal_id`
            CROSS JOIN `date_ranges` AS `dr`
        WHERE `lf`.`link_type` = 'OFFLINE'
          AND `lf`.`link_time` >= `dr`.`data_start_limit` AND `lf`.`link_time` < `dr`.`this_week_end`
    ) AS `all_visits`
    CROSS JOIN `date_ranges` AS `dr`
    GROUP BY `all_visits`.`employee_id`
),

-- =============================================================================
-- 注册数据优化 - 提前过滤和聚合, 避免使用DATE()函数
-- =============================================================================
`registration_stats` AS (
    SELECT
        `s`.`serviceman_id` AS `employee_id`,
        SUM(CASE WHEN `s`.`createdAt` >= `dr`.`last_week_start` AND `s`.`createdAt` < `dr`.`this_week_start` THEN 1 ELSE 0 END) AS `last_week_registration_count`,
        SUM(CASE WHEN `s`.`createdAt` >= `dr`.`this_week_start` AND `s`.`createdAt` < `dr`.`this_week_end` THEN 1 ELSE 0 END) AS `this_week_registration_count`,
        SUM(CASE WHEN `s`.`createdAt` >= `dr`.`yesterday_start` AND `s`.`createdAt` < `dr`.`yesterday_end` THEN 1 ELSE 0 END) AS `yesterday_registration_count`
    FROM
        `stores` AS `s`
        INNER JOIN `target_employees` AS `te` ON `te`.`employee_id` = `s`.`serviceman_id`
        CROSS JOIN `date_ranges` AS `dr`
    WHERE
        `s`.`createdAt` >= `dr`.`data_start_limit`
        AND `s`.`createdAt` < `dr`.`this_week_end`
    GROUP BY `s`.`serviceman_id`
)

-- =============================================================================
-- 最终结果 - 使用LEFT JOIN + COALESCE 处理可能不存在的数据
-- =============================================================================
SELECT
    `ei`.`subsidiary_name` AS `子公司`,
    `ei`.`employee_name` AS `经营人员`,
    
    -- 产值指标
    COALESCE(`up`.`last_week_value`, 0) AS `上一周产值`,
    COALESCE(`up`.`this_week_value`, 0) AS `本周产值`,
    COALESCE(`up`.`this_week_value`, 0) - COALESCE(`up`.`last_week_value`, 0) AS `产值周环比`,
    
    -- 毛利指标  
    COALESCE(`up`.`last_week_gross_profit`, 0) AS `上一周毛利`,
    COALESCE(`up`.`this_week_gross_profit`, 0) AS `本周毛利`,
    COALESCE(`up`.`this_week_gross_profit`, 0) - COALESCE(`up`.`last_week_gross_profit`, 0) AS `毛利周环比`,
    
    -- 日度指标
    COALESCE(`up`.`yesterday_value`, 0) AS `昨日产值`,
    
    -- 活动指标
    COALESCE(`vs`.`last_week_visit_count`, 0) AS `上周拜访次数`,
    COALESCE(`vs`.`this_week_visit_count`, 0) AS `本周拜访次数`,
    COALESCE(`vs`.`yesterday_visit_count`, 0) AS `昨日拜访数`,
    
    COALESCE(`rs`.`last_week_registration_count`, 0) AS `上周注册数`,
    COALESCE(`rs`.`this_week_registration_count`, 0) AS `本周注册数`,
    COALESCE(`rs`.`yesterday_registration_count`, 0) AS `昨日注册数`

FROM
    `employee_info` AS `ei`
    LEFT JOIN `unified_performance` AS `up` ON `ei`.`employee_id` = `up`.`employee_id`
    LEFT JOIN `visit_stats` AS `vs` ON `ei`.`employee_id` = `vs`.`employee_id`
    LEFT JOIN `registration_stats` AS `rs` ON `ei`.`employee_id` = `rs`.`employee_id`
ORDER BY `ei`.`employee_name`;