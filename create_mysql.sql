-- cat create_mysql.sql | sudo mysql
--
DROP USER IF EXISTS 'metrics'@'localhost';
DROP DATABASE IF EXISTS `metrics`;
--
CREATE DATABASE `metrics` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE USER 'metrics'@'localhost' IDENTIFIED BY 'metrics';
GRANT ALL PRIVILEGES ON `metrics`.* TO 'metrics'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
