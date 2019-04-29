CREATE DATABASE IF NOT EXISTS bigdata;
USE bigdata;

CREATE TABLE click_event (
  id          INT NOT NULL AUTO_INCREMENT
  COMMENT '自增主键',
  topic       VARCHAR(100) COMMENT '新闻主题',
  count       BIGINT COMMENT '主题计数',
  create_time BIGINT COMMENT '访问时间',
  PRIMARY KEY (id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='新闻热点统计表';


CREATE TABLE order_info (
  id        INT NOT NULL AUTO_INCREMENT
  COMMENT '自增主键'
  order_id VARCHAR (100) COMMENT '订单id',
  user_id   INT COMMENT '用户id',
  is_paid   INT COMMENT '支付状态',
  price     DOUBLE COMMENT '单价',
  paid_time DATE COMMENT '支付时间',
  PRIMARY KEY (id)
)
  ENGINE = innodb
  DEFAULT CHARSET = utf8;
