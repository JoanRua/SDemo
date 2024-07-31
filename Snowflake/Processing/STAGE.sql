CREATE OR REPLACE TABLE STAGE.stg_customer_segmentation_data
AS 
(
WITH customer_segmentation_data AS 
(
SELECT
    Customer_ID as Source_Customer_ID,
    Age ,
    Gender ,
    Marital_Status ,
    Education_Level ,
    Geographic_Information ,
    Occupation ,
    Income_Level ,
    Behavioral_Data ,
    COALESCE(
    TRY_TO_DATE(Purchase_History, 'MM-DD-YYYY'),
    TRY_TO_DATE(Purchase_History, 'MM/DD/YYYY')
    ) as Purchase_History ,
    Interactions_with_Customer_Service ,
    Insurance_Products_Owned ,
    Coverage_Amount ,
    Premium_Amount ,
    Policy_Type ,
    Customer_Preferences ,
    Preferred_Communication_Channel ,
    Preferred_Contact_Time ,
    Preferred_Language ,
    Segmentation_Group 
FROM RAW.customer_segmentation_data
)
SELECT ROW_NUMBER() OVER (ORDER BY Purchase_History, Source_Customer_ID DESC) as Customer_SK ,*
FROM customer_segmentation_data
);

CREATE TABLE STAGE.dim_customer
AS
(
SELECT
    Customer_SK,
    Source_Customer_ID,
    Age ,
    Gender ,
    Marital_Status ,
    Education_Level ,
    Geographic_Information ,
    Occupation ,
    Income_Level ,
    Behavioral_Data ,
    Purchase_History as Last_Purchase_date,
    Interactions_with_Customer_Service ,
    Insurance_Products_Owned ,
    Customer_Preferences ,
    Preferred_Communication_Channel ,
    Preferred_Contact_Time ,
    Preferred_Language ,
    Segmentation_Group 
    FROM STAGE.stg_customer_segmentation_data
)
;
CREATE TABLE STAGE.dim_geography
AS
(
WITH geography_data AS 
(
SELECT DISTINCT 
Geographic_Information as subdivision_name, 
'IN' as country
FROM STAGE.stg_customer_segmentation_data
ORDER BY Geographic_Information
)
SELECT  
CONCAT(country, '-',ROW_NUMBER() OVER (ORDER BY subdivision_name ASC)) as subdivision_id,
subdivision_name, 
country
FROM geography_data
ORDER BY subdivision_name
);


CREATE TABLE STAGE.dim_policy
as (
SELECT DISTINCT CAST(UPPER(left(Policy_Type,1)) as CHAR(1)) as Policy_id, CAST(Policy_Type as CHAR(10)) as Policy_Type
FROM STAGE.stg_customer_segmentation_data
);

CREATE TABLE STAGE.dim_insurance_products
as (
SELECT DISTINCT CAST(UPPER(right(insurance_products_owned,1)) as CHAR(1)) as insurance_product_id, 
CAST(insurance_products_owned as CHAR(10)) as insurance_product_name
FROM STAGE.stg_customer_segmentation_data
order by insurance_product_id
);


CREATE TABLE STAGE.dim_date
AS
(
SELECT DISTINCT TO_NUMBER(TO_CHAR(Purchase_History, 'YYYYMMDD'))  AS date_key, Purchase_History AS date
FROM STAGE.stg_customer_segmentation_data);

CREATE TABLE STAGE.fact_insurance_transactions
AS 
SELECT
    c.customer_sk,
    g.subdivision_id as Geographic_Id,
    p.Policy_id,
    d.date_key as Purchase_date,
    s.Insurance_Products_Owned as Insurance_Product,
    s.Coverage_Amount,
    s.Premium_Amount
FROM STAGE.stg_customer_segmentation_data s
JOIN STAGE.dim_customer c ON s.customer_sk = c.customer_sk
JOIN STAGE.dim_geography g ON s.Geographic_Information = g.subdivision_name
JOIN STAGE.dim_policy p ON s.Policy_Type = p.Policy_Type
JOIN STAGE.dim_date d ON d.date = s.Purchase_History;

