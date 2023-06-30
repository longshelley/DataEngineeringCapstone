CREATE DATABASE  IF NOT EXISTS `creditcard_capstone`;
USE `creditcard_capstone`;

/*Checking for comparison after uploading the tables*/
SELECT TRANSACTION_TYPE, SUM(TRANSACTION_VALUE) AS TOTAL_TRANSACTION_VALUE
FROM cdw_sapp_credit_card
GROUP BY TRANSACTION_TYPE
ORDER BY TOTAL_TRANSACTION_VALUE DESC;
