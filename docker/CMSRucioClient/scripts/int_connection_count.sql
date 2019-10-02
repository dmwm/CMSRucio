/*
   Then
   sql $LOGON @./test2.sql
   where $LOGON is the connect string slightly modified to remove oracle:// and the @ becomes /
*/

set TRIMSPOOL
SPOOL count.txt
set head off
select count(*) from gv$session where username='CMS_RUCIO_PROD';
quit;
