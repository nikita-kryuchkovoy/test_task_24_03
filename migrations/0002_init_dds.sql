CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.h_users
(
    "user_id" INT NOT NULL,
    "user_id_hash" VARCHAR(32) PRIMARY KEY
)

CREATE TABLE IF NOT EXISTS dds.h_letters
(
    "letter_id" INT NOT NULL,
    "letter_id_hash" VARCHAR(32) PRIMARY KEY,
)

CREATE TABLE IF NOT EXISTS dds.s_letters
(
    "letter_id_hash" VARCHAR(32) PRIMARY KEY,
    "letter_title" TEXT NOT NULL,
    "letter_body" TEXT NOT NULL
)

CREATE TABLE IF NOT EXISTS dds.l_posts
(
    "user_id_hash" VARCHAR(32) NOT NULL,
    "letter_id_hash" VARCHAR(32) NOT NULL
)
CREATE UNIQUE INDEX IF NOT EXISTS idx_l_posts
    ON dds.l_posts ("user_id_hash", "letter_id_hash");