/**
 * test sample ddb sql for mysql
 */

/**
 * create test user
 */
GRANT ALL PRIVILEGES ON *.* TO ddb_sample@localhost IDENTIFIED BY 'ddb_sample' WITH GRANT OPTION;

/**
 * create test database
 */
CREATE DATABASE sample_ddb;

/**
 * create test table
 */
CREATE TABLE tb_user_list (
  id INT AUTO_INCREMENT NOT NULL,
  parent_id VARCHAR(256),
  friend_id VARCHAR(256),
  age INT,
  date_updated DATE,
  PRIMARY KEY(id)
);
