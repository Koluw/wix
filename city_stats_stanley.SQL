USE `interview`;
DROP procedure IF EXISTS `city_stats_stanley`;

DELIMITER $$
USE `interview`$$
CREATE DEFINER=`interview_user`@`%` PROCEDURE `city_stats_stanley`()
BEGIN
	DROP TABLE IF EXISTS interview.cities_stats_stanley;
	CREATE TABLE IF NOT EXISTS interview.cities_stats_stanley(
	city_name varchar(15),
	overall_sum tinyint
	);
-- ---------------------------------------------------------
	INSERT INTO interview.cities_stats_stanley(city_name, overall_sum)
    SELECT city_name,
		   sum(cnt) / count(city_name) as overall_sum
	FROM (
	SELECT city_name,
		   date(UTC) as UTC,
		   count(duration) as CNT FROM interview.orbital_data_stanley ods
	GROUP BY city_name, date(UTC)
	) as cte
	GROUP BY cte.city_name;
END$$

DELIMITER ;
-- -----------------------------------------------------------
call city_stats_stanley();
--
SELECT sum_stats.*,
       css.overall_sum
FROM (
SELECT csbs.city,
       csbs.population,
       csbs.max_temperature,
       csbs.min_temperature
FROM interview.city_stats_beer_sheva as csbs
union all
select csta.city,
       csta.population,
       csta.max_temperature,
       csta.min_temperature
from interview.city_stats_tel_aviv csta
union all
select cse.city,
       cse.population,
       cse.max_temperature,
       cse.min_temperature
from interview.city_stats_eilat cse
union all
select csh.city,
       csh.population,
       csh.max_temperature,
       csh.min_temperature
from interview.city_stats_haifa csh) as sum_stats
LEFT JOIN cities_stats_stanley css on css.city_name = sum_stats.city