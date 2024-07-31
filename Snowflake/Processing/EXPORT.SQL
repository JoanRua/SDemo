
--- Queries to Export Data to S3
DECLARE
  res RESULTSET DEFAULT (
 SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema IN ('PROD')
  );

BEGIN
  LET table_schema STRING;
  LET table_name STRING;
  LET export_sql STRING;
  FOR rec IN res DO
    table_schema := rec.table_schema;
    table_name := rec.table_name;

    export_sql := 
      'COPY INTO @S3_PROD_DATA/prod_data' || table_schema || '/' || table_name || '.csv.gz' || 
      ' FROM (SELECT * FROM ' || table_schema || '.' || table_name || ')' ||
      ' FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '','' COMPRESSION = GZIP';

    EXECUTE IMMEDIATE :export_sql;
  END FOR;
END;

--- Queries to Export Data

SELECT * FROM PROD.DIM_CUSTOMER;
SELECT * FROM PROD.DIM_POLICY;
SELECT * FROM PROD.DIM_DATE;
SELECT * FROM PROD.DIM_GEOGRAPHY;
SELECT * FROM PROD.FACT_INSURANCE_TRANSACTIONS;
SELECT * FROM PROD.DIM_INSURANCE_PRODUCTS;