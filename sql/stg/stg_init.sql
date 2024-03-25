CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.raw_test_data
(
    "user_id" INT NOT NULL,
    "id" INT PRIMARY KEY,
    "title" TEXT NOT NULL,
    "body" TEXT NOT NULL
);