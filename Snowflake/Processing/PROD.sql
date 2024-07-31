
-----------DIM_CUSTOMER
USE SCHEMA PROD;
CREATE OR REPLACE TEMPORARY TABLE STG_DIM_CUSTOMER AS
SELECT CUSTOMER_SK, SOURCE_CUSTOMER_ID, AGE, GENDER, MARITAL_STATUS, EDUCATION_LEVEL, GEOGRAPHIC_INFORMATION, OCCUPATION, INCOME_LEVEL, BEHAVIORAL_DATA, LAST_PURCHASE_DATE, INTERACTIONS_WITH_CUSTOMER_SERVICE, INSURANCE_PRODUCTS_OWNED, CUSTOMER_PREFERENCES, PREFERRED_COMMUNICATION_CHANNEL, PREFERRED_CONTACT_TIME, PREFERRED_LANGUAGE, SEGMENTATION_GROUP
FROM STAGE.DIM_CUSTOMER;

MERGE INTO PROD.DIM_CUSTOMER AS target
USING STG_DIM_CUSTOMER AS source
ON target.CUSTOMER_SK = source.CUSTOMER_SK
WHEN MATCHED THEN UPDATE SET
target.SOURCE_CUSTOMER_ID = source.SOURCE_CUSTOMER_ID,
target.AGE = source.AGE, 
target.GENDER = source.GENDER, 
target.MARITAL_STATUS = source.MARITAL_STATUS , 
target.EDUCATION_LEVEL = source.EDUCATION_LEVEL, 
target.GEOGRAPHIC_INFORMATION = source.GEOGRAPHIC_INFORMATION, 
target.OCCUPATION = source.OCCUPATION, 
target.INCOME_LEVEL = source.INCOME_LEVEL, 
target.BEHAVIORAL_DATA = source.BEHAVIORAL_DATA, 
target.LAST_PURCHASE_DATE = source.LAST_PURCHASE_DATE , 
target.INTERACTIONS_WITH_CUSTOMER_SERVICE = source.INTERACTIONS_WITH_CUSTOMER_SERVICE, 
target.INSURANCE_PRODUCTS_OWNED = source.INSURANCE_PRODUCTS_OWNED , 
target.CUSTOMER_PREFERENCES = source.CUSTOMER_PREFERENCES, 
target.PREFERRED_COMMUNICATION_CHANNEL = source.PREFERRED_COMMUNICATION_CHANNEL, 
target.PREFERRED_CONTACT_TIME = source.PREFERRED_CONTACT_TIME, 
target.PREFERRED_LANGUAGE = source.PREFERRED_LANGUAGE, 
target.SEGMENTATION_GROUP = source.SEGMENTATION_GROUP
WHEN NOT MATCHED THEN INSERT (
  CUSTOMER_SK, SOURCE_CUSTOMER_ID, AGE, GENDER, MARITAL_STATUS, EDUCATION_LEVEL, GEOGRAPHIC_INFORMATION, OCCUPATION, INCOME_LEVEL, BEHAVIORAL_DATA, LAST_PURCHASE_DATE, INTERACTIONS_WITH_CUSTOMER_SERVICE, INSURANCE_PRODUCTS_OWNED, CUSTOMER_PREFERENCES, PREFERRED_COMMUNICATION_CHANNEL, PREFERRED_CONTACT_TIME, PREFERRED_LANGUAGE, SEGMENTATION_GROUP
) VALUES (
  source.CUSTOMER_SK, source.SOURCE_CUSTOMER_ID, source.AGE, source.GENDER, source.MARITAL_STATUS, source.EDUCATION_LEVEL, source.GEOGRAPHIC_INFORMATION, source.OCCUPATION, source.INCOME_LEVEL, source.BEHAVIORAL_DATA, source.LAST_PURCHASE_DATE, source.INTERACTIONS_WITH_CUSTOMER_SERVICE, source.INSURANCE_PRODUCTS_OWNED, source.CUSTOMER_PREFERENCES, source.PREFERRED_COMMUNICATION_CHANNEL, source.PREFERRED_CONTACT_TIME, source.PREFERRED_LANGUAGE, source.SEGMENTATION_GROUP
);



-----------DIM_DATE


USE SCHEMA PROD;
CREATE OR REPLACE TEMPORARY TABLE STG_DIM_DATE AS
SELECT DATE_KEY, DATE
FROM STAGE.DIM_DATE;


MERGE INTO PROD.DIM_DATE AS target
USING STG_DIM_DATE AS source
ON target.DATE_KEY = source.DATE_KEY
WHEN MATCHED THEN UPDATE SET
target.DATE = source.DATE
WHEN NOT MATCHED THEN INSERT (
  DATE_KEY, DATE
) VALUES (
  source.DATE_KEY, source.DATE
);

-----------DIM_GEOGRAPHY

USE SCHEMA PROD;

CREATE OR REPLACE TEMPORARY TABLE STG_DIM_GEOGRAPHY AS
SELECT SUBDIVISION_ID, SUBDIVISION_NAME, COUNTRY
FROM STAGE.DIM_GEOGRAPHY;


MERGE INTO PROD.DIM_GEOGRAPHY AS target
USING STG_DIM_GEOGRAPHY AS source
ON target.SUBDIVISION_ID = source.SUBDIVISION_ID
WHEN MATCHED THEN UPDATE SET
target.SUBDIVISION_NAME = source.SUBDIVISION_NAME,
target.COUNTRY = source.COUNTRY
WHEN NOT MATCHED THEN INSERT (
  SUBDIVISION_ID, SUBDIVISION_NAME, COUNTRY
) VALUES (
  source.SUBDIVISION_ID, source.SUBDIVISION_NAME, source.COUNTRY
);

-----------DIM_INSURANCE_PRODUCTS


USE SCHEMA PROD;
CREATE OR REPLACE TEMPORARY TABLE STG_DIM_INSURANCE_PRODUCTS AS
SELECT INSURANCE_PRODUCT_ID, INSURANCE_PRODUCT_NAME
FROM STAGE.DIM_INSURANCE_PRODUCTS;


MERGE INTO PROD.DIM_INSURANCE_PRODUCTS AS target
USING STG_DIM_INSURANCE_PRODUCTS AS source
ON target.INSURANCE_PRODUCT_ID = source.INSURANCE_PRODUCT_ID
WHEN MATCHED THEN UPDATE SET
target.INSURANCE_PRODUCT_NAME = source.INSURANCE_PRODUCT_NAME
WHEN NOT MATCHED THEN INSERT (
  INSURANCE_PRODUCT_ID, INSURANCE_PRODUCT_NAME
) VALUES (
  source.INSURANCE_PRODUCT_ID, source.INSURANCE_PRODUCT_NAME
);

-----------DIM_DIM_POLICY

USE SCHEMA PROD;
CREATE OR REPLACE TEMPORARY TABLE STG_DIM_POLICY AS
SELECT POLICY_ID, POLICY_TYPE
FROM STAGE.DIM_POLICY;


MERGE INTO PROD.DIM_POLICY AS target
USING STG_DIM_POLICY AS source
ON target.POLICY_ID = source.POLICY_ID
WHEN MATCHED THEN UPDATE SET
target.POLICY_TYPE = source.POLICY_TYPE
WHEN NOT MATCHED THEN INSERT (
  POLICY_ID, POLICY_TYPE
) VALUES (
  source.POLICY_ID, source.POLICY_TYPE
);


-----------FACT_INSURANCE_TRANSACTIONS
USE SCHEMA PROD;
CREATE OR REPLACE TEMPORARY TABLE STG_FACT_INSURANCE_TRANSACTIONS AS
SELECT CUSTOMER_SK, GEOGRAPHIC_ID, POLICY_ID, PURCHASE_DATE, INSURANCE_PRODUCT, COVERAGE_AMOUNT, PREMIUM_AMOUNT
FROM STAGE.FACT_INSURANCE_TRANSACTIONS;

MERGE INTO PROD.FACT_INSURANCE_TRANSACTIONS AS target
USING STG_FACT_INSURANCE_TRANSACTIONS AS source
ON target.CUSTOMER_SK = source.CUSTOMER_SK
WHEN MATCHED THEN UPDATE SET
target.GEOGRAPHIC_ID = source.GEOGRAPHIC_ID,
target.POLICY_ID = source.POLICY_ID, 
target.PURCHASE_DATE = source.PURCHASE_DATE, 
target.INSURANCE_PRODUCT = source.INSURANCE_PRODUCT , 
target.COVERAGE_AMOUNT = source.COVERAGE_AMOUNT, 
target.PREMIUM_AMOUNT = source.PREMIUM_AMOUNT
WHEN NOT MATCHED THEN INSERT (
  CUSTOMER_SK, GEOGRAPHIC_ID, POLICY_ID, PURCHASE_DATE, INSURANCE_PRODUCT, COVERAGE_AMOUNT, PREMIUM_AMOUNT
) VALUES (
  source.CUSTOMER_SK, source.GEOGRAPHIC_ID, source.POLICY_ID, source.PURCHASE_DATE, source.INSURANCE_PRODUCT, source.COVERAGE_AMOUNT, source.PREMIUM_AMOUNT
);
