-- :get
select *
from accounts
-- where id = ?
limit 1;

-- :create_user
INSERT INTO accounts (
  id,
  -- Comment in the middle
  username,
  email
) VALUES (
  ?, ?, ?
);

-- :update_password
UPDATE accounts
SET password = ?
WHERE id = ?;

-- ARROBAmodel Account?
-- @bind [pwd]
-- @bind [account_id]
-- :update_password2
UPDATE accounts
SET password = ?
WHERE id = ?;

-- :delete
DELETE FROM accounts WHERE id = ? AND email = ?;

-- @bind [:acc_id, :email_address]
-- :delete2
DELETE FROM accounts WHERE id = ? AND email = ?;
