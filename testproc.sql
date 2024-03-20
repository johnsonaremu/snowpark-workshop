use role sysadmin;
use demodb.dev;
use warehouse demo_Wh;

create or replace procedure emp()
returns table (id integer, title varchar)
language sql
as
declare
  select_statement varchar;
  res resultset;
begin
  select_statement := 'SELECT employee_id,title FROM  employees where employee_id<100';
  res := (execute immediate :select_statement);
  return table(res);
end;