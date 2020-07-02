/* grouse_dcl.sql: the DCL (data control language) to create oracle user for cms_deid task
   Copyright (c) 2020 University of Kansas Medical Center

   ref: https://github.com/kumc-bmi/heron/blob/master/heron_load/create_etl_acct.sql
*/

whenever sqlerror continue;
drop user &&name cascade;
whenever sqlerror exit;

CREATE USER &&name IDENTIFIED BY &&passwd DEFAULT TABLESPACE &&tspace;

-- so new schema can access previous schema
grant connect to &&name;

grant create any table to &&name;
grant drop any table to &&name;
grant delete any table to &&name;
grant select any table to &&name;
grant insert any table to &&name;
grant alter any table to &&name;
grant update any table to &&name;

grant create any view to &&name;
grant drop any view to &&name;

grant create any index to &&name;
grant alter any index to &&name;

grant select any sequence to &&name;
grant create any sequence to &&name;
grant drop any sequence to &&name;

grant create session to &&name;
grant alter session to &&name;

grant create any procedure to &&name;
grant execute any procedure to &&name;
grant execute on DBMS_RANDOM to &&name;



