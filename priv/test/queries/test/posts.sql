-- :publish_posts_by_title
UPDATE posts SET is_draft = 0 WHERE title = ?;

-- :delete_all_drafts
DELETE FROM posts WHERE is_draft = 1;
