-- SORT_BEFORE_DIFF

create table authorization_fail_3_avrofile (key int, value string) partitioned by (ds string) stored as avrofile;
set hive.security.authorization.enabled=true;

grant Create on table authorization_fail_3_avrofile to user hive_test_user;
alter table authorization_fail_3_avrofile add partition (ds='2010');

show grant user hive_test_user on table authorization_fail_3_avrofile;
show grant user hive_test_user on table authorization_fail_3_avrofile partition (ds='2010');

select key from authorization_fail_3_avrofile where ds='2010';
