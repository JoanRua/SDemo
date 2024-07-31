USE ROLE USERADMIN;

CREATE DATABASE EDW_SANTEX;
USE EDW_SANTEX;

CREATE SCHEMA RAW;
CREATE SCHEMA STAGE;
CREATE SCHEMA PROD;

CREATE WAREHOUSE DW_RAW
  WITH WAREHOUSE_SIZE = 'XSMALL'
       AUTO_SUSPEND = 30
       AUTO_RESUME = TRUE;

CREATE WAREHOUSE DW_STAGE
  WITH WAREHOUSE_SIZE = 'SMALL'
       AUTO_SUSPEND = 15
       AUTO_RESUME = TRUE;

CREATE WAREHOUSE DW_PROD
  WITH WAREHOUSE_SIZE = 'MEDIUM'
       AUTO_SUSPEND = 5
       AUTO_RESUME = TRUE
       SCALING_POLICY = 'STANDARD'; 

CREATE TAG environment values ('raw', 'stage', 'prod');
CREATE TAG sensitivity values ('public', 'confidential', 'restricted');
CREATE TAG owner values ('team_a', 'team_b'); 
CREATE TAG data_source values ('system_x', 'system_y');    
       