--https://medium.com/snowflake/snowflake-to-snowflake-establishing-secure-cross-account-connectivity-46122e0e7482

USE ROLE  sysadmin;
USE DEMODB.snowpark;
USE WAREHOUSE DEMO_WH;



-- Tareget account information:


create or replace security integration oauth_int_sqlapi2
type=oauth
enabled=true
oauth_client=CUSTOM
OAUTH_ALLOW_NON_TLS_REDIRECT_URI=true
oauth_client_type='CONFIDENTIAL'
oauth_redirect_uri='http://0.0.0.0:3000/oauth'
oauth_issue_refresh_tokens=true
oauth_refresh_token_validity=86400;


/*
select system$show_oauth_client_secrets('OAUTH_INT_SQLAPI2');
{"OAUTH_CLIENT_SECRET_2":"",
"OAUTH_CLIENT_SECRET":"",
"OAUTH_CLIENT_ID":""}
*/

-- use above client secred and id below

CREATE OR REPLACE SECURITY INTEGRATION snow_upatel_oauth
  TYPE = API_AUTHENTICATION
  AUTH_TYPE = OAUTH2
  OAUTH_CLIENT_ID = ''
  OAUTH_CLIENT_SECRET = ''
  OAUTH_TOKEN_ENDPOINT = 'https://your_snowflake_account.snowflakecomputing.com/oauth/token-request'
  OAUTH_AUTHORIZATION_ENDPOINT = 'https://your_snowflake_account.snowflakecomputing.com/oauth/authorize'
  OAUTH_ALLOWED_SCOPES = ('https://your_snowflake_account.snowflakecomputing.com')
  ENABLED = TRUE;


-- crete networking rules so only those sites are allowed (compliance)
CREATE OR REPLACE NETWORK RULE snow_access_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('your_snowflake_account.snowflakecomputing.com'
  );

-- use below link to get refresh token
-- https://community.snowflake.com/s/article/HOW-TO-OAUTH-TOKEN-GENERATION-USING-SNOWFLAKE-CUSTOM-OAUTH

CREATE OR REPLACE SECRET snow_oauth_token
  TYPE = oauth2
  API_AUTHENTICATION = snow_upatel_oauth
  OAUTH_REFRESH_TOKEN =
  'ver:2-..................................................B';


-- create exernal access integration with networking rule created above
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION snow_access_int
  ALLOWED_NETWORK_RULES = (snow_access_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (snow_oauth_token)
  ENABLED = true;




CREATE OR REPLACE FUNCTION exec_remote(account_name string, sqltext string, wh_name string, role_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'get_data'
EXTERNAL_ACCESS_INTEGRATIONS = (snow_access_int)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = snow_oauth_token)
AS
$$
import _snowflake
import requests
import json
token = _snowflake.get_oauth_access_token('cred')
session = requests.Session()
timeout = 60
def get_data(account_name, sqltext, wh_name, role_name):
    apiurl='https://'+account_name+'.snowflakecomputing.com/api/v2/statements'
    jsonBody =  {'statement': sqltext,
            'timeout': timeout,
            'warehouse': wh_name.upper(),
            'role': role_name.upper()
            }
    header = {
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Snowflake-Account": account_name,
            "X-Snowflake-Authorization-Token-Type": "OAUTH"
        }
    response = session.post(apiurl, json=jsonBody, headers=header)
    return response.json()['data']
$$;

select exec_remote('your_snowflake_account'
, 'select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.customer limit 10'
,'demo_wh'
,'sysadmin');


select
    value[0]::number c_custkey,
    value[1]::string c_name,
    value[2]::string c_address,
    value[3]::string c_nationkey,
    value[4]::string c_phone,
    value[5]::number c_acctbal,
    value[6]::string c_mktsegment,
    value[7]::string c_comment
from
    table(flatten(input =>parse_json(
       exec_remote('your_snowflake_account'
, 'select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.customer limit 10'
,'demo_wh'
,'sysadmin')) ));
