[![Coverage Status](https://coveralls.io/repos/github/renatomassaro/FeebDB/badge.svg?branch=r/add-github-actions)](https://coveralls.io/github/renatomassaro/FeebDB?branch=main)

# FeebDB

FeebDB is... hard to explain. Let's try a thought exercise:

Imagine your relational data model contains a sharding key, from which most or all your data can be sharded. Maybe that's the `company_id` or `organization_id` or even `user_id`.

Now imagine that each shard is its own, completely independent _database_.

Do you have a customer experiencing a bug? Download their database and run it locally. You will get their data (and nobody else's).

Do you have billions of entries in the `contacts` table, from which you always need to include `WHERE company_id = ? AND contact_id = ?`. Well, with a dedicated database per customer you no longer need that first clause.

What if a new database was created from scratch for _each_ test that you run? No more flakes or Sandbox or rolling back transactions.

In other words, imagine the relational model meets the actor model. Each naturally-occurring shard (within your application) now lives in an independent database.

That's what FeebDB does! Backed by SQLite, FeebDB aims to provide a framework from which you can easily build upon this pattern.

## Why?

Going down this route of "one database per client" is an extreme decision that should not be taken lightly. It's likely impossible or impractical to make such decision after a project has started. And even then, it's the kind of decision that will impact everything else in the application architecture.

There are _many_ advantages and there are _many_ disadvantages. One should be aware of _all of them_ before doing such a decision. When in doubt, _don't follow this route_.

Maybe some day I'll have the time to update this README with a proper breakdown of the pros and cons, the main pitfalls and risks, and what you are gaining and giving up when following the database architecture supported by FeebDB.

## Documentation

There is no public documentation for FeebDB, at least not as of now. It is not used by anybody, and no one should use it unless that's _exactly_ what they are looking for.

If you are curious or feeling adventurous, feel free to inspect the code or ask questions. Maybe the tests will be the closest you can find regarding documentation.


