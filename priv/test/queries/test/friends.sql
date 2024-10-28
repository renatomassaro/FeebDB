-- :get_by_id
select * from friends where id = ?;

-- :get_by_name
select * from friends where name = ?;

-- :get_all
select * from friends;

-- :add_new
insert into friends (id, name) values (?, ?);

-- :insert
insert into friends (id, name) values (?, ?);
