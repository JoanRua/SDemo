USE ROLE USERADMIN;

CREATE USER aws_integration PASSWORD = 'c*R8@>940Q6e'
[DEFAULT_ROLE = DATA_LOADER]
[DEFAULT_WAREHOUSE = DW_RAW];
GRANT ROLE DATA_LOADER TO USER aws_integration;

CREATE USER etl_integration PASSWORD = 'Y5%6J1lNd2qj';
[DEFAULT_ROLE = DATA_ENGINEER]
[DEFAULT_WAREHOUSE = DW_STAGE];
GRANT ROLE DATA_ENGINEER TO USER etl_integration;

CREATE USER joan_rua PASSWORD = 'gmQ}H)7\Pw24';
[DEFAULT_ROLE = DATA_ANALYST]
[DEFAULT_WAREHOUSE = DW_PROD];
GRANT ROLE DATA_ANALYST TO USER joan_rua;



