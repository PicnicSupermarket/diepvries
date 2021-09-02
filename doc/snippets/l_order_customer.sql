CREATE TABLE IF NOT EXISTS dv.l_order_customer (
  l_order_customer_hashkey VARCHAR(32) NOT NULL COMMENT 'Record hashkey',
  h_customer_hashkey       VARCHAR(32) NOT NULL REFERENCES dv.h_customer (h_customer_hashkey) COMMENT 'Customer hashkey',
  h_order_hashkey          VARCHAR(32) NOT NULL REFERENCES dv.h_order (h_order_hashkey) COMMENT 'Order hashkey',
  customer_id              VARCHAR     NOT NULL COMMENT 'Customer business ID',
  order_id                 VARCHAR     NOT NULL COMMENT 'Order business ID',
  r_timestamp              TIMESTAMP   NOT NULL COMMENT 'Record timestamp',
  r_source                 VARCHAR     NOT NULL COMMENT 'Record source',
  PRIMARY KEY (l_order_customer_hashkey)
);
