set pagesize 0
set linesize 200
set trimspool on
set verify off
set feedback off
select text from user_source where name = '&1' order by line;
quit;
