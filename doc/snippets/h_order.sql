CREATE TABLE IF NOT EXISTS dv.h_order (
  h_order_hashkey VARCHAR(32) NOT NULL UNIQUE COMMENT 'Record hashkey',
  r_timestamp     TIMESTAMP   NOT NULL COMMENT 'Record timestamp',
  r_source        VARCHAR     NOT NULL COMMENT 'Record source',
  order_id        VARCHAR     NOT NULL COMMENT 'Order business ID',
  PRIMARY KEY (h_order_hashkey)
);

CREATE TABLE IF NOT EXISTS dv.hs_order (
  h_order_hashkey VARCHAR(32) NOT NULL REFERENCES dv.h_order (h_order_hashkey) COMMENT 'Record hashkey',
  s_hashdiff      VARCHAR(32) NOT NULL COMMENT 'Record hashdiff',
  r_timestamp     TIMESTAMP   NOT NULL COMMENT 'Record start timestamp',
  r_timestamp_end TIMESTAMP   NOT NULL COMMENT 'Record end timestamp',
  r_source        VARCHAR     NOT NULL COMMENT 'Record source',
  create_ts       TIMESTAMP COMMENT 'Order creation timestamp',
  quantity        INTEGER COMMENT 'Order quantity',
  PRIMARY KEY (h_order_hashkey, r_timestamp)
);
