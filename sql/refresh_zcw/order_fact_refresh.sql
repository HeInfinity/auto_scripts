WITH `order_fact` AS (
	SELECT 
				 -- 联合主键
				 CONCAT_WS(
					 '-',
				 	 COALESCE ( `orders`.`id`, 1 ),
					 COALESCE ( `orders`.`delivery_receipt_id`, 1 ),
					 COALESCE ( `orders`.`store_id`, 1 ),
					 COALESCE ( `orderitems`.`product_id`, 1 ),
					 COALESCE ( `orderreturns`.`id`, 1 ) 
				 ) AS `composite_id`,
				 
				 -- 时间信息
				 DATE(`orders`.`settlement_time`) AS `settlement_date`,
				 `orders`.`settlement_time`,
				 DATE(`orders`.`delivery_time`) AS `delivery_date`,
				 `orders`.`delivery_time` AS `delivery_time`,
				 DATE(`orders`.`receive_time`) AS `receive_date`,
				 `orders`.`receive_time`,
				 DATE(`orderitems`.`sort_time`) AS `sort_date`,
				 `orderitems`.`sort_time` AS `sort_time`,
				 COALESCE(DATE(`orderreturns`.`settlement_time`), NULL) AS `return_settlement_date`,
				 COALESCE(`orderreturns`.`settlement_time`, NULL) AS `return_settlement_time`,
				 
				 -- 基础信息
				 `orders`.`id` AS `order_id`,
				 `orderreturns`.`id` AS `order_return_id`,
				 `orders`.`dc_id` AS `dc_id`,
				 `orders`.`store_id`,
				 `orderitems`.`product_id`,
				 `orders`.`site_id`,
				 `orders`.`delivery_receipt_id` AS `delivery_id`,
				 `orders`.`status` AS `order_status`,
				 `orderreturns`.`note` AS `return_reason`,
				 `orders`.`serviceman_id`,
				 `deliveryreceipts`.`deliveryman_id`,
				 `orderitems`.`sorter` AS `sortman_id`,
				  CAST(JSON_EXTRACT(`deliveryreceipts`.`location_detail`, '$.serviceman_lng') AS DECIMAL(9, 6)) AS `deliveryman_lng`,
				  CAST(JSON_EXTRACT(`deliveryreceipts`.`location_detail`, '$.serviceman_lat') AS DECIMAL(9, 6)) AS `deliveryman_lat`,
					
				 -- 配送信息
				 `deliveryreceipts`.`deliver_bonus` AS `delivery_bonus`,
				 `deliveryreceipts`.`claim_time` AS `delivery_gettime`,
				 `deliveryreceipts`.`delivery_start` AS `delivery_starttime`,
				 `deliveryreceipts`.`delivery_end` AS `delivery_endtime`,
				 `deliveryreceipts`.`outbound_time`,
				 CASE WHEN `deliveryreceipts`.`distance` = 1 THEN '1km以内'
							WHEN `deliveryreceipts`.`distance` = 2 THEN '1-2km'
							WHEN `deliveryreceipts`.`distance` = 3 THEN '2-3km'
							WHEN `deliveryreceipts`.`distance` = 4 THEN '3-4km'
							WHEN `deliveryreceipts`.`distance` = 5 THEN '4km以上'
							ELSE NULL END AS `delivery_distance`,
							
				 -- 业绩信息
				 CAST(COALESCE(`orderitems`.`unit_price`, 0) AS DECIMAL(10, 4)) AS `order_unit_price`,
				 CAST(COALESCE(`orderitems`.`quantity` * `orderitems`.`un`, 0) AS DECIMAL(10, 2)) AS `order_ud_quantity`,
				 CAST(COALESCE(`orderitems`.`deposit`, 0) AS DECIMAL(10, 2)) AS `order_deposit`,
				 CAST(COALESCE(`orderitems`.`real_unit_price`, 0) AS DECIMAL(10, 4)) AS `unit_price`,
				 CAST(COALESCE(`orderitems`.`unit_cost`, 0) AS DECIMAL(10, 2)) AS `unit_cost`,
				 CAST(COALESCE(`orderitems`.`real_ud_quantity`, 0) AS DECIMAL(10, 2)) AS `ud_quantity`,
				 CAST(COALESCE(SUM(`orderreturns`.`ud_quantity`) OVER(PARTITION BY `orderreturns`.`order_id`, `orderreturns`.`product_id`), 0) AS DECIMAL(10, 2)) AS `return_ud_quantity`,
				 
				 -- 产品信息
				 CAST(
					 CASE WHEN `products`.`ud` = '斤' THEN (COALESCE(`orderitems`.`real_ud_quantity`, 0) / 2 )
								ELSE (COALESCE(`orderitems`.`real_ud_quantity`, 0) * `products`.`weight`) END AS DECIMAL(10, 4)
				 ) AS `weight`,
				 
				 -- 刷新时间
				 `orders`.`createdAt` AS `orders_created_time`,
				 `orders`.`updatedAt` AS `orders_updated_time`
		FROM `orders`
	 INNER JOIN `orderitems`
				 ON `orderitems`.`parent_id` = `orders`.`id`
	 INNER JOIN `deliveryreceipts`
				 ON `deliveryreceipts`.`id` = `orders`.`delivery_receipt_id`
		LEFT JOIN `orderreturns`
				 ON `orderreturns`.`order_id` = `orderitems`.`parent_id`
				 AND `orderreturns`.`product_id` = `orderitems`.`product_id`
				 AND `orderreturns`.`status` = 'finished'
	 INNER JOIN `products`
				 ON `orderitems`.`product_id` = `products`.`id`
	 WHERE `orders`.`settlement_time` BETWEEN @start_date AND @end_date-- 结算日期: 匹配结算产值
)
SELECT DISTINCT *
  FROM `order_fact`
 ORDER BY `order_fact`.`order_id`,
					`order_fact`.`delivery_id`,
					`order_fact`.`store_id`,
					`order_fact`.`product_id`,
					`order_fact`.`order_return_id`;