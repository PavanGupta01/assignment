-- CREATE TABLE for mindtickle_users
CREATE TABLE mindtickle_users (
    user_id serial PRIMARY KEY,
    user_name VARCHAR (255) NOT NULL,
    active_status VARCHAR (10) NOT NULL
);

-- INSERT Sample Data
INSERT INTO mindtickle_users (user_id, user_name, active_status) VALUES
    (1, 'User1', 'active'),
    (2, 'User2', 'inactive'),
    (3, 'User3', 'active'),
    (4, 'User4', 'active');