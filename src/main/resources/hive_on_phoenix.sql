-- ln -s /usr/hdp/current/phoenix-client/phoenix-hive.jar /usr/hdp/current/hive-client/lib/
CREATE TABLE `test.test_phoenix`(
  `id` int,
  `name` string,
  `create_date` string,
  `modify_date` string)
STORED BY 'org.apache.phoenix.hive.PhoenixStorageHandler'
TBLPROPERTIES (
      "phoenix.table.name" = "test.test_hive",
      "phoenix.zookeeper.quorum" = "bigdata.t01.58btc.com,bigdata.t02.58btc.com,bigdata.t03.58btc.com",
      "phoenix.zookeeper.znode.parent" = "/hbase-secure",
      "phoenix.zookeeper.client.port" = "2181",
      "phoenix.rowkeys" = "id",
      "phoenix.column.mapping" = "id:ID,name:NAME,create_date:CREATE_DATE,modify_date:MODIFY_DATE",
      "phoenix.table.options" = "SALT_BUCKETS=10, DATA_BLOCK_ENCODING='DIFF'"
    );