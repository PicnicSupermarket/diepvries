CREATE TABLE IF NOT EXISTS dv.h_customer (
  h_customer_hashkey VARCHAR(32) NOT NULL UNIQUE COMMENT 'Record hashkey',
  r_timestamp        TIMESTAMP   NOT NULL COMMENT 'Record timestamp',
  r_source           VARCHAR     NOT NULL COMMENT 'Record source',
  customer_id        VARCHAR     NOT NULL COMMENT 'Customer business ID',
  PRIMARY KEY (h_customer_hashkey)
);

CREATE TABLE IF NOT EXISTS dv.hs_customer (
  h_customer_hashkey VARCHAR(32) NOT NULL REFERENCES dv.h_customer (h_customer_hashkey) COMMENT 'Record hashkey',
  s_hashdiff         VARCHAR(32) NOT NULL COMMENT 'Record hashdiff',
  r_timestamp        TIMESTAMP   NOT NULL COMMENT 'Record start timestamp',
  r_timestamp_end    TIMESTAMP   NOT NULL COMMENT 'Record end timestamp',
  r_source           VARCHAR     NOT NULL COMMENT 'Record source',
  firstname          VARCHAR COMMENT 'Customer first name',
  lastname           VARCHAR COMMENT 'Customer last name',
  PRIMARY KEY (h_customer_hashkey, r_timestamp)
);
