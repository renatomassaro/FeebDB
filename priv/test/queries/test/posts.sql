-- :publish_posts_by_title
UPDATE posts SET is_draft = 0 WHERE title = ?;
