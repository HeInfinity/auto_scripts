SELECT `keep_id`
FROM `keep_connection`
WHERE `keep_connection`.`update_time` BETWEEN @start_date AND @end_date;