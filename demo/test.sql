-- MySQL
-- 经营岗业绩数据分析查询
-- 统计各经营人员的产值、毛利、拜访次数、注册数等关键指标

-- 基础员工信息CTE
WITH `employee_base` AS (
    SELECT
        `e`.`id`,
        `ss`.`NAME` AS `subsidiary_name`,  -- 子公司名称
        `e`.`NAME` AS `employee_name`      -- 经营人员姓名
    FROM
        `employees` AS `e`
        LEFT JOIN `employeeroles` AS `er` ON `er`.`employee_id` = `e`.`id`
        LEFT JOIN `roles` AS `ro` ON `ro`.`id` = `er`.`role_id`
        LEFT JOIN `subsidiaries` AS `ss` ON `e`.`sub_id` = `ss`.`id`
    WHERE
        `e`.`id` IN (5714,2252,5086,6172,6602,4252,4225,7811,7904,6617,6476,6623,4050,7771,7770,7989,8411,8617,8634,8638,1867,8681,8741,8793,9067,9081,9099,9119,9128,9184,9158,7432,9197,9193,9228,9233,9241,9266,9308,9289,9344,9302,9341,9400,9407,9403)
),

-- 上周业绩数据汇总CTE
`last_week_performance` AS (
    SELECT
        `performance_summary`.`subsidiary_name`,
        `performance_summary`.`employee_name`,
        SUM(`performance_summary`.`last_week_revenue`) AS `last_week_revenue`,      -- 上一周产值
        SUM(`performance_summary`.`last_week_profit`) AS `last_week_profit`,        -- 上一周毛利
        `visit_summary`.`last_week_visits`,                                         -- 上周拜访次数
        `registration_summary`.`last_week_registrations`                            -- 上周注册数
    FROM
        (
            -- 上周订单产值和毛利
            SELECT
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `last_week_revenue`,
                SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `last_week_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
            WHERE
                `o`.`settlement_time` >= DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND `o`.`settlement_time` <= CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
            GROUP BY
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`
            
            UNION ALL
            
            -- 上周退货产值和毛利(负值)
            SELECT
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `last_week_revenue`,
                -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `last_week_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
            WHERE
                `ret`.`settlement_time` >= DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND `ret`.`settlement_time` <= CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`
        ) AS `performance_summary`
        LEFT JOIN (
            -- 上周拜访次数统计
            SELECT
                `visit_details`.`employee_name`,
                SUM(`visit_details`.`last_week_visits`) AS `last_week_visits`
            FROM
                (
                    -- 拜访记录
                    SELECT
                        `eb`.`employee_name`,
                        COUNT(`vr`.`id`) AS `last_week_visits`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `visitrecorditems` AS `vr` ON `eb`.`id` = `vr`.`visitor`
                    WHERE
                        `vr`.`visit_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                    GROUP BY
                        `eb`.`employee_name`
                    
                    UNION ALL
                    
                    -- 大客户跟进记录
                    SELECT
                        `eb`.`employee_name`,
                        COUNT(`lf`.`id`) AS `last_week_visits`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `largecsfollowups` AS `lf` ON `eb`.`id` = `lf`.`principal_id`
                    WHERE
                        `lf`.`link_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                        AND `lf`.`link_type` = 'OFFLINE'
                    GROUP BY
                        `eb`.`employee_name`
                ) AS `visit_details`
            GROUP BY
                `visit_details`.`employee_name`
        ) AS `visit_summary` ON `visit_summary`.`employee_name` = `performance_summary`.`employee_name`
        LEFT JOIN (
            -- 上周注册数统计
            SELECT 
                `eb`.`employee_name`,
                COUNT(`s`.`id`) AS `last_week_registrations`
            FROM 
                `employee_base` AS `eb`
                LEFT JOIN `stores` AS `s` ON `s`.`serviceman_id` = `eb`.`id`
            WHERE 
                `s`.`createdAt` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
            GROUP BY 
                `eb`.`employee_name`
        ) AS `registration_summary` ON `registration_summary`.`employee_name` = `performance_summary`.`employee_name`
    GROUP BY 
        `performance_summary`.`employee_name`
),

-- 本周业绩数据汇总CTE  
`current_week_performance` AS (
    SELECT
        `performance_data`.`employee_name`,
        SUM(`performance_data`.`current_week_revenue`) AS `current_week_revenue`,    -- 本周产值
        SUM(`performance_data`.`current_week_profit`) AS `current_week_profit`,      -- 本周毛利
        `visit_data`.`current_week_visits`,                                          -- 本周拜访次数
        `registration_data`.`current_week_registrations`,                            -- 本周注册数
        `yesterday_data`.`yesterday_revenue`,                                        -- 昨日产值
        `yesterday_data`.`yesterday_profit`,                                         -- 昨日毛利
        `yesterday_visit_data`.`yesterday_visits`,                                   -- 昨日拜访数
        `yesterday_reg_data`.`yesterday_registrations`,                              -- 昨日注册数
        `day_before_data`.`day_before_revenue`,                                      -- 前日产值
        `day_before_data`.`day_before_profit`                                        -- 前日毛利
    FROM
        (
            -- 本周订单产值和毛利
            SELECT
                `eb`.`employee_name`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `current_week_revenue`,
                SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `current_week_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
            WHERE
                `o`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
            
            UNION ALL
            
            -- 本周退货产值和毛利(负值)
            SELECT
                `eb`.`employee_name`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `current_week_revenue`,
                -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `current_week_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
            WHERE
                `ret`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`employee_name`
        ) AS `performance_data`
        LEFT JOIN (
            -- 本周拜访次数统计
            SELECT
                `visit_details`.`employee_name`,
                SUM(`visit_details`.`current_week_visits`) AS `current_week_visits`
            FROM
                (
                    -- 拜访记录
                    SELECT
                        `eb`.`employee_name`,
                        COUNT(`vr`.`id`) AS `current_week_visits`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `visitrecorditems` AS `vr` ON `eb`.`id` = `vr`.`visitor`
                    WHERE
                        `vr`.`visit_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                    GROUP BY
                        `eb`.`employee_name`
                    
                    UNION ALL
                    
                    -- 大客户跟进记录
                    SELECT
                        `eb`.`employee_name`,
                        COUNT(`lf`.`id`) AS `current_week_visits`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `largecsfollowups` AS `lf` ON `eb`.`id` = `lf`.`principal_id`
                    WHERE
                        `lf`.`link_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                        AND `lf`.`link_type` = 'OFFLINE'
                    GROUP BY
                        `eb`.`employee_name`
                ) AS `visit_details`
            GROUP BY
                `visit_details`.`employee_name`
        ) AS `visit_data` ON `visit_data`.`employee_name` = `performance_data`.`employee_name`
        LEFT JOIN (
            -- 本周注册数统计
            SELECT
                `eb`.`employee_name`,
                COUNT(`s`.`id`) AS `current_week_registrations`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `stores` AS `s` ON `s`.`serviceman_id` = `eb`.`id`
            WHERE
                `s`.`createdAt` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
        ) AS `registration_data` ON `registration_data`.`employee_name` = `performance_data`.`employee_name`
        LEFT JOIN (
            -- 昨日产值和毛利
            SELECT
                `yesterday_summary`.`employee_name`,
                SUM(`yesterday_summary`.`yesterday_revenue`) AS `yesterday_revenue`,
                SUM(`yesterday_summary`.`yesterday_profit`) AS `yesterday_profit`
            FROM
                (
                    -- 昨日订单产值和毛利
                    SELECT
                        `eb`.`employee_name`,
                        SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `yesterday_revenue`,
                        SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `yesterday_profit`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                        LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
                    WHERE
                        `o`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                    GROUP BY
                        `eb`.`employee_name`
                    
                    UNION ALL
                    
                    -- 昨日退货产值和毛利(负值)
                    SELECT
                        `eb`.`employee_name`,
                        -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `yesterday_revenue`,
                        -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `yesterday_profit`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                        LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
                    WHERE
                        `ret`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                        AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
                    GROUP BY
                        `eb`.`employee_name`
                ) AS `yesterday_summary`
            GROUP BY
                `yesterday_summary`.`employee_name`
        ) AS `yesterday_data` ON `yesterday_data`.`employee_name` = `performance_data`.`employee_name`
        LEFT JOIN (
            -- 前日产值和毛利
            SELECT
                `day_before_summary`.`employee_name`,
                SUM(`day_before_summary`.`day_before_revenue`) AS `day_before_revenue`,
                SUM(`day_before_summary`.`day_before_profit`) AS `day_before_profit`
            FROM
                (
                    -- 前日订单产值和毛利
                    SELECT
                        `eb`.`employee_name`,
                        SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `day_before_revenue`,
                        SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `day_before_profit`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                        LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
                    WHERE
                        `o`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 3 DAY), '23:59:59')
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                    GROUP BY
                        `eb`.`employee_name`
                    
                    UNION ALL
                    
                    -- 前日退货产值和毛利(负值)
                    SELECT
                        `eb`.`employee_name`,
                        -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `day_before_revenue`,
                        -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `day_before_profit`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                        LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
                    WHERE
                        `ret`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 3 DAY), '23:59:59')
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                        AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
                    GROUP BY
                        `eb`.`employee_name`
                ) AS `day_before_summary`
            GROUP BY
                `day_before_summary`.`employee_name`
        ) AS `day_before_data` ON `performance_data`.`employee_name` = `day_before_data`.`employee_name`
        LEFT JOIN (
            -- 昨日拜访次数
            SELECT
                `yesterday_visit_summary`.`employee_name`,
                SUM(`yesterday_visit_summary`.`yesterday_visits`) AS `yesterday_visits`
            FROM
                (
                    -- 昨日拜访记录
                    SELECT
                        `eb`.`employee_name`,
                        COUNT(`vr`.`id`) AS `yesterday_visits`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `visitrecorditems` AS `vr` ON `eb`.`id` = `vr`.`visitor`
                    WHERE
                        `vr`.`visit_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                    GROUP BY
                        `eb`.`employee_name`
                    
                    UNION ALL
                    
                    -- 昨日大客户跟进记录
                    SELECT
                        `eb`.`employee_name`,
                        COUNT(`lf`.`id`) AS `yesterday_visits`
                    FROM
                        `employee_base` AS `eb`
                        LEFT JOIN `largecsfollowups` AS `lf` ON `eb`.`id` = `lf`.`principal_id`
                    WHERE
                        `lf`.`link_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                    GROUP BY
                        `eb`.`employee_name`
                ) AS `yesterday_visit_summary`
            GROUP BY
                `yesterday_visit_summary`.`employee_name`
        ) AS `yesterday_visit_data` ON `performance_data`.`employee_name` = `yesterday_visit_data`.`employee_name`
        LEFT JOIN (
            -- 昨日注册数
            SELECT
                `eb`.`employee_name`,
                COUNT(`s`.`id`) AS `yesterday_registrations`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `stores` AS `s` ON `s`.`serviceman_id` = `eb`.`id`
            WHERE
                `s`.`createdAt` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
        ) AS `yesterday_reg_data` ON `performance_data`.`employee_name` = `yesterday_reg_data`.`employee_name`
    GROUP BY
        `performance_data`.`employee_name`
),

-- 上周首单业绩数据CTE
`last_week_first_order` AS (
    SELECT
        `first_order_summary`.`employee_name`,
        SUM(`first_order_summary`.`last_week_first_revenue`) AS `last_week_first_revenue`,  -- 上周首单产值
        SUM(`first_order_summary`.`last_week_first_profit`) AS `last_week_first_profit`,    -- 上周首单毛利
        `first_order_count`.`last_week_first_count`                                         -- 上周首单数
    FROM
        (
            -- 上周首单订单产值和毛利
            SELECT
                `eb`.`employee_name`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `last_week_first_revenue`,
                SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `last_week_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` >= DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND `o`.`settlement_time` <= CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 2 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
            
            UNION ALL
            
            -- 上周首单退货产值和毛利(负值)
            SELECT
                `eb`.`employee_name`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `last_week_first_revenue`,
                -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `last_week_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `ret`.`settlement_time` >= DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND `ret`.`settlement_time` <= CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 2 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`employee_name`
        ) AS `first_order_summary`
        LEFT JOIN (
            -- 上周首单数统计
            SELECT
                `eb`.`employee_name`,
                COUNT(DISTINCT `o`.`store_id`) AS `last_week_first_count`
            FROM
                `employee_base` AS `eb`
                INNER JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                INNER JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` >= DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND `o`.`settlement_time` <= CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 2 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 8 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
        ) AS `first_order_count` ON `first_order_count`.`employee_name` = `first_order_summary`.`employee_name`
    GROUP BY
        `first_order_summary`.`employee_name`
),

-- 本周首单业绩数据CTE
`current_week_first_order` AS (
    SELECT
        `first_order_summary`.`employee_name`,
        SUM(`first_order_summary`.`current_week_first_revenue`) AS `current_week_first_revenue`,  -- 本周首单产值
        SUM(`first_order_summary`.`current_week_first_profit`) AS `current_week_first_profit`,    -- 本周首单毛利
        `first_order_count`.`current_week_first_count`                                             -- 本周首单数
    FROM
        (
            -- 本周首单订单产值和毛利
            SELECT
                `eb`.`employee_name`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `current_week_first_revenue`,
                SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `current_week_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
            
            UNION ALL
            
            -- 本周首单退货产值和毛利(负值)
            SELECT
                `eb`.`employee_name`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `current_week_first_revenue`,
                -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `current_week_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `ret`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`employee_name`
        ) AS `first_order_summary`
        LEFT JOIN (
            -- 本周首单数统计
            SELECT
                `eb`.`employee_name`,
                COUNT(DISTINCT `o`.`store_id`) AS `current_week_first_count`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
        ) AS `first_order_count` ON `first_order_count`.`employee_name` = `first_order_summary`.`employee_name`
    GROUP BY
        `first_order_summary`.`employee_name`
),

-- 日产值历史数据CTE(用于周环比计算)
`daily_revenue_history` AS (
    SELECT
        `daily_summary`.`subsidiary_name`,
        `daily_summary`.`employee_name`,
        `daily_summary`.`revenue_date`,
        SUM(`daily_summary`.`daily_revenue`) AS `daily_revenue`
    FROM
        (
            -- 日订单产值
            SELECT
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`,
                DATE_FORMAT(`o`.`settlement_time`, '%Y-%m-%d') AS `revenue_date`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `daily_revenue`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
            WHERE
                `o`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 5 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`,
                `revenue_date`
            
            UNION ALL
            
            -- 日退货产值(负值)
            SELECT
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`,
                DATE_FORMAT(`ret`.`settlement_time`, '%Y-%m-%d') AS `revenue_date`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `daily_revenue`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
            WHERE
                `ret`.`settlement_time` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 5 WEEK)
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`subsidiary_name`,
                `eb`.`employee_name`,
                `revenue_date`
        ) AS `daily_summary`
    GROUP BY
        `daily_summary`.`subsidiary_name`,
        `daily_summary`.`employee_name`,
        `daily_summary`.`revenue_date`
),

-- 昨日首单数据CTE
`yesterday_first_order` AS (
    SELECT
        `yesterday_first_summary`.`employee_name`,
        SUM(`yesterday_first_summary`.`yesterday_first_revenue`) AS `yesterday_first_revenue`,  -- 昨日首单产值
        SUM(`yesterday_first_summary`.`yesterday_first_profit`) AS `yesterday_first_profit`,    -- 昨日首单毛利
        `yesterday_first_count`.`yesterday_first_count`                                         -- 昨日首单数
    FROM
        (
            -- 昨日首单订单产值和毛利
            SELECT
                `eb`.`employee_name`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `yesterday_first_revenue`,
                SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `yesterday_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
            
            UNION ALL
            
            -- 昨日首单退货产值和毛利(负值)
            SELECT
                `eb`.`employee_name`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `yesterday_first_revenue`,
                -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `yesterday_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `ret`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`employee_name`
        ) AS `yesterday_first_summary`
        LEFT JOIN (
            -- 昨日首单数统计
            SELECT
                `eb`.`employee_name`,
                COUNT(DISTINCT `o`.`store_id`) AS `yesterday_first_count`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 3 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
        ) AS `yesterday_first_count` ON `yesterday_first_count`.`employee_name` = `yesterday_first_summary`.`employee_name`
    GROUP BY
        `yesterday_first_summary`.`employee_name`
),

-- 前日首单数据CTE
`day_before_first_order` AS (
    SELECT
        `day_before_first_summary`.`employee_name`,
        SUM(`day_before_first_summary`.`day_before_first_revenue`) AS `day_before_first_revenue`,  -- 前日首单产值
        SUM(`day_before_first_summary`.`day_before_first_profit`) AS `day_before_first_profit`      -- 前日首单毛利
    FROM
        (
            -- 前日首单订单产值和毛利
            SELECT
                `eb`.`employee_name`,
                SUM(`oi`.`real_unit_price` * `oi`.`real_ud_quantity`) AS `day_before_first_revenue`,
                SUM((`oi`.`real_unit_price` - `oi`.`unit_cost`) * `oi`.`real_ud_quantity`) AS `day_before_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `orderitems` AS `oi` ON `oi`.`parent_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `o`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 3 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
            GROUP BY
                `eb`.`employee_name`
            
            UNION ALL
            
            -- 前日首单退货产值和毛利(负值)
            SELECT
                `eb`.`employee_name`,
                -SUM(`ret`.`money` - `ret`.`deposit` * `ret`.`deposit_quantity`) AS `day_before_first_revenue`,
                -SUM(`ret`.`ud_quantity` * (`ret`.`unit_price` - `ret`.`unit_cost`)) AS `day_before_first_profit`
            FROM
                `employee_base` AS `eb`
                LEFT JOIN `orders` AS `o` ON `eb`.`id` = `o`.`serviceman_id`
                LEFT JOIN `OrderReturns` AS `ret` ON `ret`.`order_id` = `o`.`id`
                LEFT JOIN `userportraits` AS `ut` ON `ut`.`store_id` = `o`.`store_id`
            WHERE
                `ret`.`settlement_time` BETWEEN CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 3 DAY), '23:59:59')
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 2 DAY), '23:59:59')
                AND `ut`.`first_order_date` BETWEEN LAST_DAY(DATE_SUB(NOW(), INTERVAL 1 MONTH))
                AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59')
                AND `ret`.`STATUS` NOT IN ('retDc', 'dcRet', 'created', 'confirmed', 'audited', 'rejected')
            GROUP BY
                `eb`.`employee_name`
        ) AS `day_before_first_summary`
    GROUP BY
        `day_before_first_summary`.`employee_name`
)

-- 主查询: 汇总所有指标数据
SELECT
    `lwp`.`subsidiary_name`,                                                                        -- 子公司
    `lwp`.`employee_name`,                                                                          -- 经营人员
    `lwp`.`last_week_revenue`,                                                                      -- 上一周产值
    `cwp`.`current_week_revenue`,                                                                   -- 本周产值
    (`cwp`.`current_week_revenue` - `lwp`.`last_week_revenue`) AS `revenue_wow`,                    -- 产值周环比
    `lwfo`.`last_week_first_revenue`,                                                               -- 上周首单产值
    `cwfo`.`current_week_first_revenue`,                                                            -- 本周首单产值
    (`cwfo`.`current_week_first_revenue` - `lwfo`.`last_week_first_revenue`) AS `first_revenue_wow`, -- 首单产值周环比
    `lwp`.`last_week_profit`,                                                                       -- 上一周毛利
    `cwp`.`current_week_profit`,                                                                    -- 本周毛利
    (`cwp`.`current_week_profit` - `lwp`.`last_week_profit`) AS `profit_wow`,                       -- 毛利周环比
    `lwfo`.`last_week_first_profit`,                                                                -- 上周首单毛利
    `cwfo`.`current_week_first_profit`,                                                             -- 本周首单毛利
    (`cwfo`.`current_week_first_profit` - `lwfo`.`last_week_first_profit`) AS `first_profit_wow`,   -- 首单毛利周环比
    -- 周产值环比计算 (基于日产值历史数据)
    (SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 4 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 3 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END) - 
     SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 5 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 4 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END)) AS `week_1_revenue_wow`,                             -- 第一周产值环比
    (SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 3 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 2 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END) - 
     SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 4 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 3 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END)) AS `week_2_revenue_wow`,                             -- 第二周产值环比
    (SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 2 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END) - 
     SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 3 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 2 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END)) AS `week_3_revenue_wow`,                             -- 第三周产值环比
    (SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK) 
        AND CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59') 
        THEN `drh`.`daily_revenue` ELSE 0 END) - 
     SUM(CASE 
        WHEN `drh`.`revenue_date` BETWEEN DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 2 WEEK) 
        AND DATE_SUB(CONCAT_WS(' ', DATE_SUB(CURDATE(), INTERVAL 1 DAY), '23:59:59'), INTERVAL 1 WEEK) 
        THEN `drh`.`daily_revenue` ELSE 0 END)) AS `week_4_revenue_wow`,                             -- 第四周产值环比
    `yfo`.`yesterday_first_revenue`,                                                                 -- 昨日首单产值
    `yfo`.`yesterday_first_profit`,                                                                  -- 昨日首单毛利
    `dbfo`.`day_before_first_revenue`,                                                               -- 前日首单产值
    `dbfo`.`day_before_first_profit`,                                                                -- 前日首单毛利
    `lwp`.`last_week_visits`,                                                                        -- 上周拜访次数
    `cwp`.`current_week_visits`,                                                                     -- 本周拜访次数
    `lwp`.`last_week_registrations`,                                                                 -- 上周注册数
    `cwp`.`current_week_registrations`,                                                              -- 本周注册数
    `lwfo`.`last_week_first_count`,                                                                  -- 上周首单数
    `cwfo`.`current_week_first_count`,                                                               -- 本周首单数
    `cwp`.`yesterday_revenue`,                                                                       -- 昨日产值
    `cwp`.`day_before_revenue`,                                                                      -- 前日产值
    `cwp`.`yesterday_visits`,                                                                        -- 昨日拜访数
    `cwp`.`yesterday_registrations`,                                                                 -- 昨日注册数
    `yfo`.`yesterday_first_count`                                                                    -- 昨日首单数
FROM
    `employee_base` AS `eb`
    LEFT JOIN `last_week_performance` AS `lwp` ON `eb`.`employee_name` = `lwp`.`employee_name`
    LEFT JOIN `current_week_performance` AS `cwp` ON `eb`.`employee_name` = `cwp`.`employee_name`
    LEFT JOIN `last_week_first_order` AS `lwfo` ON `eb`.`employee_name` = `lwfo`.`employee_name`
    LEFT JOIN `current_week_first_order` AS `cwfo` ON `eb`.`employee_name` = `cwfo`.`employee_name`
    LEFT JOIN `daily_revenue_history` AS `drh` ON `eb`.`employee_name` = `drh`.`employee_name`
    LEFT JOIN `yesterday_first_order` AS `yfo` ON `eb`.`employee_name` = `yfo`.`employee_name`
    LEFT JOIN `day_before_first_order` AS `dbfo` ON `eb`.`employee_name` = `dbfo`.`employee_name`
GROUP BY 
    `eb`.`employee_name`; 