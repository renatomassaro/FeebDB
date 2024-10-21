drop index testbango;
alter table people add column email text;
alter table people drop column should_not_exist;
create table foo (id int);
