create table people (
  id integer primary key,
  name text,
  should_not_exist text
);

create index testbango on people (name);
