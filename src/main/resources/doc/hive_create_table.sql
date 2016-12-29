use dw_dimensions;
CREATE EXTERNAL TABLE dim_web_location(
web_location_key    BIGINT,
ip_section_1        INT,
ip_section_2        INT,
ip_section_3        INT,
country             STRING,
area                STRING,
province            STRING,
city                STRING,
district            STRING,
city_level          STRING,
longitude           DOUBLE,
latitude            DOUBLE,
isp                 STRING
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_web_location';

CREATE EXTERNAL TABLE dim_date(
date_key        STRING,
year            INT,
month           INT,
day_of_month    INT,
day_of_year     INT,
day_of_week     INT,
day_type        STRING
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_date';

CREATE EXTERNAL TABLE dim_time(
time_key        STRING,
hour            INT,
minute          INT,
second          INT,
period          STRING
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_time';

CREATE EXTERNAL TABLE dim_app_version(
app_version_key       BIGINT,
app_name              STRING,
app_en_name           STRING,
app_id                STRING,
app_series            STRING,
version               STRING,
build_time            STRING,
company               STRING,
product               STRING
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_app_version';

CREATE EXTERNAL TABLE dim_app_channel(
app_channel_sk        BIGINT,
channel_code          STRING,
channel_name          STRING
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_app_channel';

CREATE EXTERNAL TABLE dim_medusa_account(
account_sk        BIGINT,
account_id        STRING,
user_name         STRING,
email             STRING,
mobile            STRING,
reg_time          TIMESTAMP,
register_from     STRING
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_medusa_account';

CREATE EXTERNAL TABLE dim_medusa_terminal_user(
terminal_sk         BIGINT,
user_id             STRING,
open_time           TIMESTAMP,
mac                 STRING,
wifi_mac            STRING,
product_model       STRING,
product_serial      STRING,
promotion_channel   STRING,
last_login_time     TIMESTAMP
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_medusa_terminal_user';

CREATE EXTERNAL TABLE dim_medusa_program(
program_sk          BIGINT,
sid                 STRING,
title               STRING,
content_type        STRING,
duration            INT,
video_type          INT,
parent_sid          STRING,
episode_index       INT,
area                STRING,
year                INT,
video_length_type   INT,
create_time         TIMESTAMP,
publish_time        TIMESTAMP
)
STORED AS PARQUET
LOCATION '/data_warehouse/dw_dimensions/dim_medusa_program';