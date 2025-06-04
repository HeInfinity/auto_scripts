WITH `tags_info` AS (
	SELECT `tags`.`id` AS `tagtype_id`,
				 `tags`.`name` AS `tagtpye_name`,
				 `tagtypes`.`id` AS `tag_id`,
				 `tagtypes`.`name` AS `tag_name`
		FROM `tags`
	 INNER JOIN `tagtypes`
				 ON `tagtypes`.`id` = `tags`.`type_id`
),
`store_tags_double` AS (
	SELECT *,
				 ROW_NUMBER() OVER(PARTITION BY `storetags`.`store_id` ORDER BY `storetags`.`tag_id`) AS `store_tag_order`
		FROM `storetags`
),
`store_tags_single` AS ( -- 未发现重复值
	SELECT *
		FROM `store_tags_double`
	 WHERE `store_tags_double`.`store_tag_order` = 1
),
`store` AS (
	SELECT DISTINCT
				 `stores`.`dc_id`,
				 `stores`.`id` AS `store_id`,
				 `stores`.`name` AS `store_name`,
				 `stores`.`salesman_id`,
				 `stores`.`serviceman_id`,
				 `stores`.`createdAt` AS `registrated_date`,
				 `stores`.`transfer_deal_at` AS `transfer_date`,
				 `stores`.`lng` AS `store_lng`,
				 `stores`.`lat` AS `store_lat`,
				 `stores`.`delivery_route_id` AS `delivery_routeid`,
				 `stores`.`business_type` AS `operating_status`,
				 `stores`.`old_serviceman_id`,
				 `stores`.`deliver_bonus`,
				 `stores`.`updatedAt` AS `store_updated_time`
		FROM `stores`
	 WHERE `stores`.`updatedAt` BETWEEN @start_date AND @end_date
),
`store_info` AS ( -- 未发现重复值
	SELECT `store`.*,
				 `store_tags_single`.`tag_id`
		FROM `store_tags_single`
	 RIGHT JOIN `store`
				 USING (`store_id`)
),
`store_tags` AS (
	SELECT `store_info`.`dc_id`,
				 `store_info`.`store_id`,
				 `store_info`.`store_name`,
				 `store_info`.`salesman_id`,
				 `store_info`.`serviceman_id`,
				 `store_info`.`registrated_date`,
				 `store_info`.`transfer_date`,
				 `tags_info`.`tag_id`,
				 `tags_info`.`tag_name`,
				 `store_info`.`store_lng`,
				 `store_info`.`store_lat`,
				 `store_info`.`delivery_routeid`,
				 `store_info`.`operating_status`,
				 `store_info`.`old_serviceman_id`,
				 `store_info`.`deliver_bonus`,
				 `store_info`.`store_updated_time`
		FROM `tags_info`
	 RIGHT JOIN `store_info`
				 ON `store_info`.`tag_id` = `tags_info`.`tagtype_id`
),
`order_sort` AS ( -- 求首单时间/末单时间前置辅助表
	SELECT `orders`.`store_id`,
				 `orders`.`createdAt` AS `first_order_date`,
				 `orders`.`serviceman_id` AS `first_serviceman_id`, -- 首单数: 使用`first_serviceman_id`对`first_settlement_date`进行COUNT
				 `orders`.`createdAt` AS `last_order_date`,
				 `orders`.`serviceman_id` AS `last_serviceman_id`,
				 `orders`.`dc_id` AS `first_dc_id`,
				 `orders`.`dc_id` AS `last_dc_id`,
				 ROW_NUMBER() OVER(PARTITION BY `orders`.`store_id` ORDER BY `orders`.`id`) AS `sort`,
				 ROW_NUMBER() OVER(PARTITION BY `orders`.`store_id` ORDER BY `orders`.`id` DESC) AS `sort_desc`
		FROM `orders`
	 WHERE `orders`.`serviceman_id` <> 0
   	 AND ( `orders`.`settlement_time` <> '1970-01-01 00:00:00' OR `orders`.`status` NOT IN ("CANCELED", "EXPIRED") )
		 AND `orders`.`store_id` IN
					(
						SELECT `store_tags`.`store_id`
						  FROM `store_tags`
					)
),
`order_firsted` AS ( -- 客户信息表(仅含首单结算信息){[store_id][first_order_settlement_date][first_order_serviceman_id]}
	SELECT *
		FROM `order_sort`
	 WHERE `order_sort`.`sort` = 1
),
`order_lasted` AS ( -- 客户信息表(仅含首单结算信息){[store_id][first_order_settlement_date][first_order_serviceman_id]}
	SELECT *
		FROM `order_sort`
	 WHERE `order_sort`.`sort_desc` = 1
),
`Result` AS (
	SELECT CASE WHEN `store_tags`.`tag_id` IS NULL THEN 21 ELSE `store_tags`.`tag_id` END AS `tag_id`,
				 CASE WHEN `store_tags`.`tag_name` IS NULL THEN "其他（零售/食堂/饮品）" ELSE `store_tags`.`tag_name` END AS `tag_name`,
				 `store_tags`.`store_id`,
				 `store_tags`.`store_name`,
				 `store_tags`.`salesman_id` AS `registratman_id`,
				 `order_firsted`.`first_serviceman_id`,
				 `order_lasted`.`first_serviceman_id` AS `last_serviceman_id`,
				 `store_tags`.`old_serviceman_id` AS `previous_serviceman_id`,
				 `store_tags`.`serviceman_id`,
				 `store_tags`.`registrated_date` AS `registrated_time`,
				 DATE(`store_tags`.`registrated_date`) AS `registrated_date`,
				 `order_firsted`.`first_order_date` AS `first_order_time`,
				 DATE(`order_firsted`.`first_order_date`) AS `first_order_date`,
				 `order_lasted`.`last_order_date` AS `last_order_time`,
				 DATE(`order_lasted`.`last_order_date`) AS `last_order_date`,
				 `store_tags`.`transfer_date` AS `transfer_time`,
				 DATE(`store_tags`.`transfer_date`) AS `transfer_date`,
				 COALESCE( `order_firsted`.`first_dc_id`, `store_tags`.`dc_id` ) AS `first_dc_id`,
				 COALESCE( `order_lasted`.`last_dc_id`, `store_tags`.`dc_id` ) AS `last_dc_id`,
				 CAST(`store_tags`.`store_lng` AS DECIMAL(9, 6)) AS `store_lng`,
				 CAST(`store_tags`.`store_lat` AS DECIMAL(9, 6)) AS `store_lat`,
				 `store_tags`.`delivery_routeid`,
				 `store_tags`.`operating_status`,
				 CAST(`delivercenters`.`lng` AS DECIMAL(9, 6)) AS `dc_lng`,
				 CAST(`delivercenters`.`lat` AS DECIMAL(9, 6)) AS `dc_lat`,
				 `store_tags`.`deliver_bonus`,
				 `store_tags`.`store_updated_time`
		FROM `store_tags`
	  LEFT JOIN `order_firsted`
				 ON `store_tags`.`store_id` = `order_firsted`.`store_id`
	  LEFT JOIN `order_lasted`
				 ON `store_tags`.`store_id` = `order_lasted`.`store_id`
	  LEFT JOIN `delivercenters`
				 ON `store_tags`.`dc_id` = `delivercenters`.`id`
),
`Result_Distance` AS (
SELECT `Result`.*,
				CAST(6371 * ACOS(COS(RADIANS(`Result`.`store_lat`)) * COS(RADIANS(`Result`.`dc_lat`)) * COS(RADIANS(`Result`.`store_lng`) - RADIANS(`Result`.`dc_lng`)) + SIN(RADIANS(`Result`.`store_lat`)) * SIN(RADIANS(`Result`.`dc_lat`))) AS DECIMAL(9, 4)) AS `distance_km`
  FROM `Result`
)
SELECT *
  FROM `Result_Distance`
 ORDER BY `Result_Distance`.`store_id`;