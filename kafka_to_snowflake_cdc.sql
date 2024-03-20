--https://medium.com/snowflake/snowflake-achieving-lightning-fast-end-to-end-data-pipeline-86193e00ceb9
use role kafka_connector_role;
use streamdb.kafka;
use warehouse kafka_wh;

-- table automatically created by Kafka Connector


TRUNCATE TABLE kafka.events_data;

select * from kafka.events_data limit 100;

create or replace table ad_campaign_map (ad_id integer, campaign varchar);
insert into ad_campaign_map (ad_id, campaign) values
    (1, 'winter_sports'),
    (2, 'winter_sports'),
    (3, 'spring_break'),
    (4, 'memorial_day'),
    (5, 'youth_in_action'),
    (6, 'youth_in_action'),
    (7, 'memorial_day'),
    (8, 'youth_in_action'),
    (9, 'spring_break'),
    (10, 'winter_sports'),
    (11, 'building_community'),
    (12, 'youth_on_course'),
    (13, 'youth_on_course'),
    (14, 'fathers_day'),
    (15, 'fathers_day'),
    (16, 'fathers_day'),
    (17, 'summer_olympics'),
    (18, 'winter_olympics'),
    (19, 'women_in_sports'),
    (20, 'women_in_sports'),
    (21, 'mothers_day'),
    (22, 'super_bowl'),
    (23, 'stanley_cup'),
    (24, 'nba_finals'),
    (25, 'world_series'),
    (26, 'world_cup'),
    (27, 'uefa'),
    (28, 'family_history'),
    (29, 'thanksgiving_football'),
    (30, 'sports_across_cultures');


-- final table to do analytics
CREATE OR REPLACE TABLE  MODEL.ADCLICK_DATA (
    RECORD_METADATA VARIANT ,
    AD_ID NUMBER,
    COST FLOAT,
    CHANNEL VARCHAR,
    CLICK NUMBER,
    ORIG_TS TIMESTAMP ,
    LAST_TS TIMESTAMP DEFAULT current_timestamp
);

-- build pipeline

-- create stream

CREATE OR REPLACE
STREAM   kafka.events_data_stream
ON TABLE kafka.events_data
    append_only = true ;

-- triggered task
CREATE or REPLACE TASK kafka.process_events
    WAREHOUSE = kafka_wh
    USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 15
WHEN
    SYSTEM$STREAM_HAS_DATA('kafka.events_data_stream')
AS
BEGIN
     INSERT INTO MODEL.ADCLICK_DATA (RECORD_METADATA, AD_ID, COST, CHANNEL,CLICK,ORIG_TS)
       SELECT RECORD_METADATA, AD_ID, COST, CHANNEL,CLICK, timeadd(hour,-8,to_timestamp(TIMESTAMP::number(38,6))) ts
       FROM KAFKA.EVENTS_DATA_STREAM;
     COMMIT;
END;


-- activate task

ALTER TASK kafka.process_events resume;



-- run kafka program to push messeages

-- kafka ingestion count
SELECT
    (select count(*) from kafka.events_data) as table_count,
    (SELECT count(*) FROM  kafka.events_data_stream) as stream_count
;



-- snowpipe stream latency
with a  as
(
    select  TO_TIMESTAMP_ltz(to_number(record_metadata:CreateTime)/1000) cts,
            to_timestamp_ltz(to_number(timestamp)) ots,
            datediff('ms',ots,cts) latency
    from streamdb.kafka.events_data
)
select count(*) cnt, round(avg(abs(latency)/100)) lat  from a;

-- end to end latency
select (AVG(datediff('second',orig_ts,last_ts))) e2elat from STREAMDB.MODEL.ADCLICK_DATA;



-- stat and latency
SELECT
    (SELECT COUNT(*) FROM kafka.events_data) as current_table,
    (SELECT COUNT(*) FROM MODEL.ADCLICK_DATA) as model_table,
    (
        with a  as
        (
         select  TO_TIMESTAMP_ltz(to_number(record_metadata:CreateTime)/1000) cts,
              to_timestamp_ltz(to_number(timestamp)) ots,
             datediff('ms',ots,cts) latency
     from streamdb.kafka.events_data
)
        select round(avg(abs(latency)/100)) lat  from a
    ) as k2s_lat,
    (SELECT AVG(datediff('second',orig_ts,last_ts)) from MODEL.ADCLICK_DATA) as end2end_lat,
    (SELECT COUNT(*) FROM kafka.events_data AT(OFFSET => -30)) as sec30_ago,
    (SELECT COUNT(*) FROM kafka.events_data AT(OFFSET => -60)) as mins_ago,
     current_table-mins_ago as min_ago,
    (SELECT COUNT(*) FROM kafka.events_data AT(OFFSET => -120)) as two_mins_ago,
    (SELECT COUNT(*) FROM kafka.events_data AT(OFFSET => -300)) as five_mins_ago

 ;


--- Let's create a DT that joins our raw ad data with the campaign info,
-- calculates the spend/CPC and total click count grouped by day, campaign, and channel

create or replace DYNAMIC TABLE
    model.campaign_spend_daily
lag = '1 minute'
warehouse = KAFKA_WH AS
    select c.campaign, p.channel, to_date(p.orig_ts) ad_date,
        sum(p.click) total_clicks,
        sum(p.cost) total_cost,
        sum(1) ads_served
    from
        kafka.ad_campaign_map c,
        model.adclick_data p
    where
        c.ad_id = p.ad_id
    group by
        c.campaign, p.channel, ad_date;


alter dynamic table model.campaign_spend_daily refresh;

select * from model.campaign_spend_daily limit 100;

select * from model.campaign_spend_daily order by ad_date desc;

select sum(ads_served) from model.campaign_spend_daily;

show channels in kafka.events_data;



-- STOP PIPELINE
alter dynamic table  model.campaign_spend_daily suspend;

ALTER TASK kafka.process_events suspend;

drop dynamic table model.campaign_spend_daily;
