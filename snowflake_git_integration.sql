-- article
-- https://medium.com/snowflake/streamlining-devops-with-snowflake-and-git-integration-fc0b76a40a76

USE ROLE sysadmin;
USE demodb.dev;

use role accountadmin;

CREATE  SECRET IF NOT EXISTS upatel_gh_auth
    TYPE = password
    username  = 'sfc-gh-upatel'
    password  = 'your pat';
    --https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
-- create integration with git

CREATE OR REPLACE api integration umeshrepo_int
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/yourrepo')
    allowed_authentication_secrets=ALL
    enabled=TRUE;


SHOW API INTEGRATIONS;

CREATE OR REPLACE git repository upatel_snowpark_workshop
    api_integration= umeshrepo_int
    git_credentials = upatel_gh_auth
    origin = 'https://github.com/yourrepo/snowpark_workshop';

-- make sure you use branches/main while doing list
LS @upatel_snowpark_workshop/branches/main;

-- see your latest updated file
SELECT "name",
       "size",
       to_timestamp("last_modified", 'dy, dd mon yyyy hh24:mi:ss GMT') lastupdated
FROM table(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY 3 DESC ;

create or replace function FAKE(locale varchar,provider varchar,parameters variant)
returns variant
language python
volatile
runtime_version = '3.8'
packages = ('faker','simplejson')
handler = 'fake'
as
$$
import simplejson as json
from faker import Faker
def fake(locale,provider,parameters):
  if type(parameters).__name__=='sqlNullWrapper':
    parameters = {}
  fake = Faker(locale=locale)
  return json.loads(json.dumps(fake.format(formatter=provider,**parameters), default=str))
$$;

CREATE OR REPLACE TABLE  customers AS
SELECT
    ABS(RANDOM()) AS CUSTOMER_ID,
    FAKE('en_US','first_name',null)::varchar AS FIRST_NAME,
    FAKE('en_US','last_name',null)::varchar AS LAST_NAME,
    FAKE('en_US','phone_number',null)::varchar AS PHONE_NO,
    FAKE('en_US','free_email',null)::varchar AS EMAIL,
    FAKE('en_US','state_abbr',null)::varchar AS STATE
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

select * from customers;

create or replace table  products as
    SELECT
     seq4()::number id,
     demo_db.demo.fakeValue('commerce','department') as department,
     demo_db.demo.fakeValue('commerce','productName') as productName,
     demo_db.demo.fakeValue('company','name') as CompanyName,
     demo_db.demo.fakeValue('commerce','price')::NUMBER as price,
     demo_db.demo.fakeValue('commerce','promotionCode') as promotionCode
    from table(generator(rowcount => 1000));

select * from products limit 100;


CREATE OR REPLACE PROCEDURE hello_git ()
    returns table ()
    language python
    runtime_version='3.8'
    packages= ('snowflake-snowpark-python')
    imports= ('@upatel_snowpark_workshop/branches/main/gittest.py')
    handler= 'gittest.hello';

call hello_git()    ;

ls @upatel_snowpark_workshop/branches/main;

-- Further development in vscode

-- show updated file
SELECT   "name", "size", to_timestamp("last_modified",'dy, dd mon yyyy hh24:mi:ss GMT') last_modified
    FROM table(RESULT_SCAN(LAST_QUERY_ID())) order by 3 desc ;

-- fetch latest code
alter git repository upatel_snowpark_workshop fetch;
call hello_git() ;



EXECUTE IMMEDIATE FROM '@upatel_snowpark_workshop/branches/main/testproc.sql';
call emp();


show streamlits in account;

-- create streamlit application
create or replace streamlit tb_streamlit_app
root_location = @demodb.dev.upatel_snowpark_workshop/branches/main/
main_file = '/tastybyte_sis.py'
query_warehouse = 'DEMO_WH';


show git branches in git repository upatel_snowpark_workshop;
