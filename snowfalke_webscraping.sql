--https://medium.com/snowflake/loading-data-from-github-or-public-website-to-snowflake-331009d378c2
use role sysadmin;
use demodb.extfunc;

--https://docs.snowflake.com/en/release-notes/requirements#recommended-client-versions

CREATE NETWORK RULE if not exists  SNOWFLAKE_DOCS_EXTERNAL_ACCESS
   TYPE = HOST_PORT
   VALUE_LIST = ( 'docs.snowflake.com' )
   MODE = EGRESS
   COMMENT = 'docs.snowflake.com';

CREATE EXTERNAL ACCESS INTEGRATION if not exists SNOWFLAKE_DOCS_EXTERNAL_ACCESS_INT
  ALLOWED_NETWORK_RULES = ( SNOWFLAKE_DOCS_EXTERNAL_ACCESS )
  ENABLED = TRUE;


CREATE OR REPLACE FUNCTION get_snowflake_recommended_versions()
RETURNS  TABLE (
                type string,
                client  string,
                ver string,
                minver string,
                endver string,
                rel string,
                download string

               )
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'get_snowflake_recommended_versions'
EXTERNAL_ACCESS_INTEGRATIONS = (SNOWFLAKE_DOCS_EXTERNAL_ACCESS_INT)
PACKAGES = ('urllib3','beautifulsoup4','pandas','html5lib')
AS
$$
import _snowflake
import urllib3
import pandas as pd
import html5lib
from bs4 import BeautifulSoup
http = urllib3.PoolManager()
URL='https://docs.snowflake.com/en/release-notes/requirements#recommended-client-versions'
class get_snowflake_recommended_versions:
    def process(self):
        resp = http.request("GET", URL)
        soup = BeautifulSoup(resp.data.decode(), "html.parser")
        tables = pd.read_html(str(soup),flavor='html5lib',encoding='utf-8')
        # versions comptibilty is the first table in the page
        ver_table = tables[0]
        yield from ver_table.to_records(index=False).tolist()
$$;

select * from table(get_snowflake_recommended_versions());
