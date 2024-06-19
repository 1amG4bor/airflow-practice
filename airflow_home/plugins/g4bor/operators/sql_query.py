drop_table_users = r"DROP TABLE IF EXISTS users_{0} CASCADE;"

create_table_users = r"""
    CREATE TABLE users_{0} (
    id              INTEGER PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(100),
    active          BOOLEAN,
    ip_address      VARCHAR(16),
    country         VARCHAR(100),
    gender          VARCHAR(10),
    os_platform     VARCHAR(10));
"""

display_table_users = r"SELECT * FROM users_{0};"

drop_table_users_by_location_GOLD = "DROP TABLE IF EXISTS users_by_location_gold;"
drop_table_users_by_device_GOLD = "DROP TABLE IF EXISTS users_by_device_gold;"
drop_table_inactive_users_GOLD = "DROP TABLE IF EXISTS inactive_users_by_country_and_platform_gold;"

users_distributed_by_location_GOLD ="""
CREATE TABLE users_by_location_gold AS
(SELECT pivot.country as Country,
	   COALESCE(SUM(pivot.male), 0) as Male_users,
	   COALESCE(SUM(pivot.female), 0) as Female_users
FROM crosstab(
	$$SELECT country, gender, count(*) AS users FROM users_silver GROUP BY country,gender$$,
	$$ values ('Male'), ('Female') $$)
AS pivot (country TEXT, Male BIGINT, Female BIGINT) GROUP BY country ORDER BY country)
"""

users_distributed_by_device_GOLD ="""
CREATE TABLE users_by_device_gold AS
(SELECT pivot.os_platform as OS_platform,
	   COALESCE(SUM(pivot.male), 0) as Male_users,
	   COALESCE(SUM(pivot.female), 0) as Female_users
FROM crosstab(
	$$SELECT os_platform, gender, count(*) AS users FROM users_silver GROUP BY os_platform, gender$$,
	$$ values ('Male'), ('Female') $$)
AS pivot (os_platform TEXT, Male BIGINT, Female BIGINT) GROUP BY os_platform ORDER BY os_platform)
"""

inactive_users_by_country_and_platform_GOLD = """
CREATE TABLE inactive_users_by_country_and_platform_gold AS
(SELECT country, os_platform, count(*) as inactive_users
    FROM users_silver WHERE active is false
    GROUP BY country, os_platform ORDER BY country, os_platform)
"""