use role sysadmin;
use demodb.extfunc;

-- networking rules
CREATE OR REPLACE NETWORK RULE web_access_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api-web.nhle.com');
 --https://api-web.nhle.com/v1/standings/2024-03-01
-- These are the external domains that we want Snowflake to be able to access.

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION web_access_integration
  ALLOWED_NETWORK_RULES = (web_access_rule)
  ENABLED = true;

use role sysadmin;
CREATE OR REPLACE FUNCTION ext_access_test(urlstr string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'getdata'
EXTERNAL_ACCESS_INTEGRATIONS = (web_access_integration)
PACKAGES = ('urllib3')
AS
$$
import _snowflake
import urllib3
def getdata(URLSTR):
    http = urllib3.PoolManager()
    resp = http.request("GET", URLSTR)
    return str(resp.json()) # decode method to fix the return string
$$;

select add_months(current_Date,-1);

select parse_json(ext_access_test('https://api-web.nhle.com/v1/standings/'||add_months(current_Date,-1)));



with nhlstanding as (
select parse_json(ext_access_test('https://api-web.nhle.com/v1/standings/'||add_months(current_Date,-1))) data
)
select
  'NHL' league
, value:conferenceName::varchar confname
, value:divisionName::varchar divanme
, value:teamName.default::varchar teamname
, value:teamAbbrev.default::varchar abbr
, value:gamesPlayed::number gp
, value:wins::number::number w
, value:losses::number l
, value:otLosses::number ot
, value:points::number points
, value:pointPctg::number pper
, value:leagueSequence::number rank
, value
from nhlstanding s,
lateral flatten (input=>s.data:"standings")
order by 1, 2, points desc, gp;


-- Create the NHL table
CREATE OR REPLACE TABLE DATAAPP.STREAMLITAPP.NHL (
    LEAGUE STRING,
    CONFERENCE STRING,
    DIVISION STRING,
    TEAM STRING,
    TEAM_ABBV STRING,
    GP NUMBER,
    W NUMBER,
    L NUMBER,
    OT NUMBER,
    PTS NUMBER,
    PTS_PCT DECIMAL,
    rank number,
    data variant
);

select current_Date;

insert overwrite into DATAAPP.STREAMLITAPP.NHL
with nhlstanding as (
select parse_json(ext_access_test('https://api-web.nhle.com/v1/standings/'||current_date)) data
)
select
  'NHL' league
, value:conferenceName::varchar confname
, value:divisionName::varchar divname
, value:teamName.default::varchar teamname
, value:teamAbbrev.default::varchar abbr
, value:gamesPlayed::number gp
, value:wins::number::number w
, value:losses::number l
, value:otLosses::number ot
, value:points::number points
, value:pointPctg::number pper
, value:leagueSequence::number rank
, value
from nhlstanding s,
lateral flatten (input=>s.data:"standings")
order by 1, 2, points desc, gp;


CREATE OR REPLACE procedure DATAAPP.STREAMLITAPP.nhl_standing(asof string )
returns TABLE (
                league string,
                conference  string,
                division string,
                team string,
                team_abbv string,
                gp number,
                w number,
                l number,
                ot number,
                pts number,
                pts_pct number,
                rank number
               )
language sql
as
declare
  select_statement varchar;
  res resultset;
begin
  select_statement := '
          with nhlstanding as (
        select parse_json(demodb.extfunc.ext_access_test(\'https://api-web.nhle.com/v1/standings/'||asof||'\')) data
        )
        select
         \'NHL\' LEAGUE
        , value:conferenceName::varchar CONFERENCE
        , value:divisionName::varchar DIVISION
        , value:teamName.default::varchar TEAM
        , value:teamAbbrev.default::varchar TEAM_ABBV
        , value:gamesPlayed::number GP
        , value:wins::number::number W
        , value:losses::number L
        , value:otLosses::number OT
        , value:points::number PTS
        , value:pointPctg::number PTS_PCT
        , value:leagueSequence::number RANK
        from nhlstanding s,
        lateral flatten (input=>s.data:"standings")
        order by 1, 2, pts desc, gp'    ;
  res := (execute immediate :select_statement);
  return table(res);
end;

call nhl_standing(to_char(current_date));
