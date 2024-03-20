--https://medium.com/snowflake/simplifying-data-ingestion-creating-a-data-pipeline-in-snowflake-with-sftp-e99033f230c2
/*
use role sysadmin;
create event table event_logs;

SHOW PARAMETERS LIKE 'event_table' IN ACCOUNT;
use role accountadmin;
alter account set event_Table = demodb.public.event_logs;
alter database dataload set log_level=info;
*/

USE ROLE sysadmin;
USE DATABASE dataload;
use schema cdc;
USE WAREHOUSE DEMO_WH;


CREATE  SECRET if not exists sftp_aws_cred
    TYPE = password
    USERNAME = 'sftp_username'
    PASSWORD = '-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAACFwAAAAdzc2gtcn
keK1wp9Zjf82VZ3N41te8VpAFs5EjVVue3
683+H3R+whcAAAAAAQID
-----END OPENSSH PRIVATE KEY-----
'
;

CREATE NETWORK RULE if not exists aws_sftp_network_rule
  TYPE = HOST_PORT
  VALUE_LIST = ('your.sftpserver.com:22')
  -- Port 22 is the default for SFTP, change if your port is different
  MODE= EGRESS
;

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sftp_aws_ext_int
  ALLOWED_NETWORK_RULES = (aws_sftp_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (sftp_aws_cred)
  ENABLED = true
;


--drop procedure  LOAD_FROM_SFTP(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMBER) ;
CREATE OR REPLACE PROCEDURE   load_from_sftp
        (stage_name string, stage_dir string, sftp_remote_path string, pattern string, sftp_server string ,port integer )
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'getfiles'
EXTERNAL_ACCESS_INTEGRATIONS = (sftp_aws_ext_int)
PACKAGES = ('snowflake-snowpark-python','pysftp','re2')
SECRETS = ('cred' = sftp_aws_cred)
AS
$$
import _snowflake
import pysftp
import re
import os
from snowflake.snowpark.files import SnowflakeFile
import logging
logger = logging.getLogger("sftp_logger")
def getfiles(session, internal_stage, stage_dir,   remote_file_path, pattern, sftp_server, port):
    sftp_cred = _snowflake.get_username_password('cred');
    sftp_host = sftp_server
    sftp_port = port
    sftp_username = sftp_cred.username
    sftp_privatekey = sftp_cred.password
    privkeyfile = '/tmp/content' + str(os.getpid())
    with open(privkeyfile, "w") as file:
      file.write(sftp_privatekey)
    full_path_name = f'{internal_stage}/{stage_dir}'
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    try:
        with pysftp.Connection(host=sftp_host, username=sftp_username, private_key=privkeyfile, port=sftp_port, cnopts=cnopts) as sftp:
            if sftp.exists(remote_file_path):
                sftp.chdir(remote_file_path)
                rdir=sftp.listdir()
                ret=[]
                for file in (rdir):
                    if re.search(pattern,file) != None:
                        sftp.get(file, f'/tmp/{file}')
                        session.file.put(f'/tmp/{file}', full_path_name, auto_compress=False, overwrite=True )
                        ret.append(file)
        logger.info("Files downloaded from sftp server "+sftp_host+"Files: ".join(ret))
        return ret
    except Exception as e:
        return f" Error with SFTP : {e}"
$$;


CREATE FILE FORMAT  if not exists opps_csv
    type = 'csv',
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ',',
    SKIP_HEADER = 1,
    DATE_FORMAT = 'AUTO'
;

create or replace stage opps_stage file_format = opps_csv;

/* see file in the server
sftp -o "IdentityFile=~/.ssh/sftp_rsa.pem" upatel@your.sftpserver.com

*/


-- test it, load data from sftp to internal stage
CALL load_from_sftp('@opps_stage',  -- stage name
                    'sftpdir', -- stage directory
                    '',  -- remote file path of sftp server, root
                    'opp', -- file prefix
                    'your.sftpserver.com',22);

LS @opps_stage;

-- create table through infer schema
CREATE OR REPLACE TRANSIENT TABLE  opps_rawdata
(
    opp_id number,
    company_name varchar,
    close_date date,
    opp_stage varchar,
    opp_name varchar,
    opp_desc varchar,
    opp_amount number,
    prob number(4)
);


CREATE OR REPLACE STREAM opps_rawdata_stream
ON TABLE opps_rawdata APPEND_ONLY=TRUE;

select * from opps_rawdata_stream;



-- test it to make sure data can be loaded
 COPY INTO opps_rawdata  from @opps_stage PATTERN = '.*csv.*';


select * from opps_rawdata order by opp_id;
SELECT * from opps_rawdata_stream order by opp_id;



-- create a final table that analyst want to use
CREATE OR REPLACE TABLE  opportunity
(
    opp_id number,
    company_name varchar,
    close_date date,
    opp_stage varchar,
    opp_name varchar,
    opp_desc varchar,
    opp_amount number,
    prob number(4),
    last_updated timestamp default current_timestamp,
    last_updated_by varchar
);


-- schedule to load data from sftp
CREATE or replace TASK load_from_sftp_task
  schedule = '1 minute'
  warehouse = demo_wh
as
BEGIN
    CALL load_from_sftp('@opps_stage',
                    'sftpdir','','opp',
                    'your.sftpserver.com',22);
    COPY INTO opps_rawdata from @opps_stage  PATTERN = '.*csv.*';
END;

-- test it

execute task load_from_sftp_task;

select * from opps_rawdata_stream;


-- notice serverless triggred task
CREATE or REPLACE TASK update_opp_task
  WAREHOUSE = DEMO_WH
  USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 15
WHEN
  SYSTEM$STREAM_HAS_DATA('opps_rawdata_stream')
AS
  MERGE INTO opportunity tgt
  USING (
    SELECT *
    FROM opps_rawdata_stream
  ) src
  ON tgt.opp_id = src.opp_id
   and ( tgt.opp_stage <> src.opp_stage
       or  tgt.opp_amount <> src.opp_amount
       or  tgt.prob <> src.prob
       or  tgt.opp_stage <> src.opp_stage
       or  tgt.close_date <> src.close_date
    )
  -- data update condition
  WHEN MATCHED THEN UPDATE SET
      tgt.close_date = src.close_date
      , tgt.opp_stage = src.opp_stage
      , tgt.opp_amount = src.opp_amount
      , tgt.opp_desc = src.opp_desc
      , tgt.prob  = src.prob
      , tgt.last_updated = current_timestamp()
      , tgt.last_updated_by = 'TASK Update:update_opp_task '
  WHEN NOT MATCHED THEN INSERT (
         opp_id
      ,  company_name
      , close_date
      , opp_stage
      , opp_name
      , opp_desc
      , opp_amount
      , prob
      , last_updated
      , last_updated_by
    ) VALUES (
        src.opp_id
      ,  src.company_name
      , src.close_date
      , src.opp_stage
      , src.opp_name
      , src.opp_desc
      , src.opp_amount
      , src.prob
      , current_timestamp
      , 'TASK Insert:update_opp_task '
    );

-- test it
execute task update_opp_task;


CREATE OR REPLACE TASK purge_opp_stage_data
  WAREHOUSE = DEMO_WH
  AFTER update_opp_task
AS
    EXECUTE IMMEDIATE $$
    BEGIN
        TRUNCATE TABLE opps_rawdata;
        RM @opps_stage PATTERN = '.*sftpdir/opp.*';
    END;
$$;



select * from opps_rawdata_stream;
select * from opportunity;
ls @opps_stage;

ALTER TASK load_from_sftp_task RESUME;
ALTER TASK purge_opp_stage_data RESUME;
ALTER TASK update_opp_task RESUME;



SHOW TASKS;


-- When will the next task run
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history())
where state = 'SCHEDULED'
order by completed_time desc;

-- You will see only one task - LOAD_FROM_SFTP_TASK
-- other will be triggered autatically

-- Show task history

select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10
   ))
    order by scheduled_time desc;

 -- test your pipeline
select count(*) from opportunity;
select * from opportunity order by opp_id;


-- scinario 2 - load excel file from sftp

CALL load_from_sftp('@opps_stage',  -- stage name
                    'xls', -- stage directory
                    '',  -- remote file path of sftp server, root
                    'xls', -- file prefix
                    'your.sftpserver.com',22);


ls @opps_Stage;


CREATE OR REPLACE PROCEDURE load_excel(excel_file STRING, sheet_name STRING, table_number INT, table_gap INT, rows_to_skip INT, target_table_name STRING)
returns STRING
language python runtime_version = 3.8
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl','et_xmlfile')
handler = 'main'
as
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.types import Variant,StringType,VariantType, IntegerType
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import os
import logging
logger = logging.getLogger("loadexcel_logger")

# -------------- MAIN ---------------
def main(session: snowpark.Session, excel_file, sheet_name, table_number, table_gap, rows_to_skip, target_table_name):

    # Get file from stage
    filename = os.path.basename(excel_file)
    staged_file = session.file.get(excel_file, "/tmp")
    xls_full_path = f"/tmp/{filename}"

    skip_header_rows = rows_to_skip

    for x in range(table_number):
        xls_df = pd.read_excel(xls_full_path ,sheet_name=sheet_name, header=skip_header_rows)

        # Detect end of table (if there are multiple tables in a single sheet)
        try:
            range_end = xls_df[xls_df.isnull().all(axis=1) == True].index.tolist()[0]
        except:
            range_end = len(xls_df)

        # Drop column if all values for the column are null
        xls_df = xls_df.loc[0:range_end-1].dropna(axis=1, how='all')

        skip_header_rows = skip_header_rows + len(xls_df) + table_gap + 1


    # Create Snowpark dataframe
    snowpark_df = session.create_dataframe(xls_df)
    snowpark_df.write.mode('overwrite').save_as_table(target_table_name)
    logger.info("Table "+target_table_name+" created from  file "+xls_full_path)
    return_msg = f"SUCCESS. "

    return return_msg


$$;




ls @opps_Stage;

-- load excel into snowflake
call load_excel('@opps_stage/xls/boats.xlsx', 'boats', 1, 0, 0, 'boats');
select *  from boats;


-- check logs
select scope,record_type,value, resource_attributes, timestamp
from demodb.public.event_logs order by timestamp desc limit 10;



-- cleanup

ALTER TASK update_opp_task SUSPEND;
ALTER TASK purge_opp_stage_data SUSPEND;
alter task load_from_sftp_task suspend;

DROP TASK update_opp_task;
DROP TASK purge_opp_stage_data;
DROP TASK  load_from_sftp_task;
