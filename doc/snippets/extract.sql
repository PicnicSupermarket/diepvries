CREATE OR REPLACE TABLE dv_extract.order_customer (
  customer_id VARCHAR,
  firstname   VARCHAR,
  lastname    VARCHAR,
  order_id    VARCHAR,
  create_ts   VARCHAR,
  quantity    VARCHAR
);

INSERT INTO dv_extract.order_customer
VALUES
  ('1', 'Alice', 'Doe', '1', '2021-03-17T14:00:00+00:00', '2'),
  ('1', 'Alice', 'Doe', '2', '2021-03-17T15:00:00+00:00', '3'),
  ('1', 'Alice', 'Doe', '3', '2021-03-17T16:00:00+00:00', '2'),
  ('2', 'Bob', 'Smith', '4', '2021-03-17T17:00:00+00:00', '8'),
  (NULL, 'Charlie', NULL, '4', '2021-03-17T18:00:00+00:00', '8');

SELECT * FROM dv_extract.order_customer;
